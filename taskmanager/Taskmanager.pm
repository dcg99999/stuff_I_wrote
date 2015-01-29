package Edgewater::Taskmanager;

=head1 NAME

Edgewater::Taskmanager - Edgeview Task Manager

=head1 AUTHOR

Dan Gray, Edgewater Networks, Inc., dgray@edgewaternetworks.com

=head1 DESCRIPTION

B<Edgewater::Taskmanager> provides a Perl object to the main Edgewater Taskmanager Interface

=cut

use strict;
use ewn_db;
use ewn_notify;
use Edgewater::Mail;

# constructor
########################################################
sub new
{
 my $class = shift;	# what class are we constructing?
 my $self = {};		# allocate new memory

 bless($self,$class);	# bless to make an object
 $self->_init(@_);	# call _init with remaining args
 return $self;
}
########################################################
# _init is a "private" method to initialize fields.  If invoked with
# arguments, _init interprets them as key+value pairs to initialize
# the object with.  Remember, anything that starts with an underscore
# is a private method.
########################################################
sub _init
{
  my $self = shift;
  
  # set any default values here...
  # doing this is not absolutely necessary, but I like to show all
  # the object elements in one spot so it is easier to work with
  $self->{TASKID} = undef;  # Set when AddTaskToQueue is called
  $self->{COMMAND} = undef;
  $self->{ARGUMENTS} = undef;
  $self->{STATUS} = undef;
  $self->{TIMEOUT} = "10";
  $self->{PRIORITY} = "0";
  $self->{ISHEAVY} = 0;
  $self->{NOTIFICATION_EMAILS} = "";
  $self->{UPDATE_TABLE} = undef;
  $self->{UPDATE_TABLE_ID_COLUMN} = undef;
  $self->{UPDATE_TABLE_ID_VALUE} = undef;
  $self->{UPDATE_TABLE_STATUS_COLUMN} = undef;
  $self->{UPDATE_TABLE_QUEUED_STATUS_VALUE} = 1;
  $self->{UPDATE_TABLE_EXECUTED_STATUS_VALUE} = 100;
  $self->{UPDATE_TABLE_STARTED_STATUS_VALUE} = 2;
  $self->{UPDATE_TABLE_ERROR_STATUS_VALUE} = 99;
  $self->{UPDATE_TABLE_TIMEOUT_STATUS_VALUE} = 98;
  $self->{UPDATE_TABLE_CANCELLED_STATUS_VALUE} = 97;
  $self->{UPDATE_TABLE_COMMENT_COLUMN} = undef;
  
  # here, if they passed key => value during constructor, it gets set
  if( @_ ) {
    my %extra = @_;
    @$self{map uc, keys %extra} = values %extra; # make keys always capitals!
  }
  
}
########################################################
# debug method that just dumps the object hash key+values
########################################################
sub show_object
{
  my $self = shift;
  
  foreach my $k (sort keys %$self) {
    my $v = $self->{$k};
    print "[$k] => [$v]\n";
  }
}


sub Debug($) {
  my $arg = shift;
  open (X,">>/tmp/debug.out");
  my $date = `date`;
  chomp($date);
  print X "[".$date."]: ".$arg."\n";
  close X;
}

sub LookupTask($) {
  my $taskid = shift;

  # Returns a task object for a given task ID

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("SELECT command,arguments,timeout,priority,isheavy,notification_emails,update_table,update_table_id_column,update_table_id_value,update_table_status_column,update_table_queued_status_value,update_table_started_status_value,update_table_executed_status_value,update_table_error_status_value,update_table_timeout_status_value,update_table_cancelled_status_value,update_table_comment_column FROM tasks where taskid=$taskid") || die $dbh->errstr;

  $sth->execute();

  my ($command,$arguments,$timeout,$priority,$isheavy,$notification_emails,$update_table,$update_table_id_column,$update_table_id_value,$update_table_status_column,$update_table_queued_status_value,$update_table_started_status_value,$update_table_executed_status_value,$update_table_error_status_value,$update_table_timeout_status_value,$update_table_cancelled_status_value,$update_table_comment_column) = $sth->fetchrow();

  $sth->finish();
  $dbh->disconnect();

  if (defined $command) {
    return new Edgewater::Taskmanager
    (TASKID=>$taskid,
     COMMAND=>$command,
     ARGUMENTS=>$arguments,
     PRIORITY=>$priority,
     ISHEAVY=>$isheavy,
     NOTIFICATION_EMAILS=>$notification_emails,
     TIMEOUT=>$timeout,
     UPDATE_TABLE=>$update_table,
     UPDATE_TABLE_ID_COLUMN=>$update_table_id_column,
     UPDATE_TABLE_ID_VALUE=>$update_table_id_value,
     UPDATE_TABLE_STATUS_COLUMN=>$update_table_status_column,
     UPDATE_TABLE_QUEUED_STATUS_VALUE=>$update_table_queued_status_value,
     UPDATE_TABLE_STARTED_STATUS_VALUE=>$update_table_started_status_value,
     UPDATE_TABLE_EXECUTED_STATUS_VALUE=>$update_table_executed_status_value,
     UPDATE_TABLE_ERROR_STATUS_VALUE=>$update_table_error_status_value,
     UPDATE_TABLE_TIMEOUT_STATUS_VALUE=>$update_table_timeout_status_value,
     UPDATE_TABLE_CANCELLED_STATUS_VALUE=>$update_table_cancelled_status_value,
     UPDATE_TABLE_COMMENT_COLUMN=>$update_table_comment_column
    );
  }
  else {
    return undef;
  }
}


sub LookupTaskIDByUpdateID($$$) {
  my ($update_table,$update_column,$update_value) = @_;

  # Returns a task object for a given task ID

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("SELECT taskid FROM tasks where update_table=? and update_table_id_column=? and update_table_id_value=? order by taskid desc limit 1") || die $dbh->errstr;
  $sth->execute($update_table,$update_column,$update_value);

  my ($id) = $sth->fetchrow();

  $sth->finish();
  $dbh->disconnect();

  if (defined $id) {
    return $id;
  }
  else {
    return undef;
  }
}


sub RequeueTask($$) {
  my $self = shift;
  my $schedule = shift;

  my $dbh=ewn_db::get_dbh(retry=>100);

  #Debug("In Requeue, setting status = queued for taskid=".$self->{TASKID}." and schedule=$schedule");
  if (!defined $schedule || $schedule eq "") {
    my $sth = $dbh->prepare("UPDATE tasks SET status=?,schedule=Now() WHERE taskid=$self->{TASKID}");
    $sth->execute("QUEUED") || die $dbh->errstr;
    $sth->finish();
  }
  else {
    my $sth = $dbh->prepare("UPDATE tasks SET status=?,schedule=? WHERE taskid=$self->{TASKID}");
    $sth->execute("QUEUED",$schedule) || die $dbh->errstr;

    $sth->finish();
  }
 
  $dbh->disconnect();

  $self->SetUpdateTableQueuedStatus();

  $self->SetUpdateTableComment("Requeued in Task Manager");

  return $self;
}


sub RequeueTaskLater($$) {
  my $self = shift;
  my $minuteslater = shift;

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("SELECT NOW()+INTERVAL ? MINUTE");
  $sth->execute($minuteslater) || die $dbh->errstr;
  my $dt = $sth->fetchrow();
  $sth->finish();
  $dbh->disconnect();

  if (defined $dt) {
    return $self->RequeueTask($dt);
  }

  die $dbh->errstr();

}

sub GetSchedule($) {
  my $self = shift;

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("SELECT schedule from tasks WHERE taskid=$self->{TASKID}");
  $sth->execute() || die $dbh->errstr;
  my $schedule = $sth->fetchrow();
  $sth->finish();
 
  $dbh->disconnect();

  return $schedule;
}

sub CancelTask($;$) {
  my $self = shift;
  my $notifymessage=shift;

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE tasks SET status=? WHERE taskid=$self->{TASKID} and status=\"QUEUED\"");
  $sth->execute("CANCELLED") || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

  $self->SetUpdateTableCancelledStatus();

  $self->SetUpdateTableComment("Task Cancelled");

  if (!defined $notifymessage && $self->{NOTIFICATION_EMAILS} ne "") {
    $self->SendEmails("CANCELLED","");
  }
  elsif ($self->{NOTIFICATION_EMAILS} ne "") {
    $self->SendEmails("CANCELLED",$notifymessage);
  }


  return $self;
}

sub CancelTaskWithoutNotify($) {
  my $self = shift;

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE tasks SET status=? WHERE taskid=$self->{TASKID} and status=\"QUEUED\"");
  $sth->execute("CANCELLED") || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

  $self->SetUpdateTableCancelledStatus();

  $self->SetUpdateTableComment("Task Cancelled");

  return $self;
}

sub AddTaskToQueue($$) {
  my $self=shift;
  my $schedule=shift;

  # Lock the task table
  my $dbh = ewn_db::get_dbh(retry=>100);

  $dbh->do("LOCK TABLES tasks WRITE") || die $dbh->errstr;
  
  my $schedulestr="";
  if (defined $schedule && $schedule ne "") {
    $schedulestr = "schedule=\"$schedule\",";
  }

  # Insert the task
  my $sth = $dbh->prepare("INSERT INTO tasks SET command=?, arguments=?, status=\"QUEUED\",timeout=?,priority=?,isheavy=?,notification_emails=?, $schedulestr update_table=?,update_table_id_column=?,update_table_id_value=?,update_table_status_column=?,update_table_queued_status_value=?,update_table_started_status_value=?,update_table_executed_status_value=?,update_table_error_status_value=?,update_table_timeout_status_value=?,update_table_cancelled_status_value=?,update_table_comment_column=?") || die $dbh->errstr;

  $sth->execute
    (
     $self->{COMMAND},
     $self->{ARGUMENTS},
     $self->{TIMEOUT},
     $self->{PRIORITY},
     $self->{ISHEAVY},
     $self->{NOTIFICATION_EMAILS},
     $self->{UPDATE_TABLE},
     $self->{UPDATE_TABLE_ID_COLUMN},
     $self->{UPDATE_TABLE_ID_VALUE},
     $self->{UPDATE_TABLE_STATUS_COLUMN},
     $self->{UPDATE_TABLE_QUEUED_STATUS_VALUE},
     $self->{UPDATE_TABLE_STARTED_STATUS_VALUE},
     $self->{UPDATE_TABLE_EXECUTED_STATUS_VALUE},
     $self->{UPDATE_TABLE_ERROR_STATUS_VALUE},
     $self->{UPDATE_TABLE_TIMEOUT_STATUS_VALUE},
     $self->{UPDATE_TABLE_CANCELLED_STATUS_VALUE},
     $self->{UPDATE_TABLE_COMMENT_COLUMN}

    ) || die $dbh->errstr;

  $self->{TASKID} = $sth->{mysql_insertid};

  $sth->finish();

  $dbh->do("UNLOCK TABLES") || die $dbh->errstr;

  # Update the caller's table if defined
  if (defined $self->{UPDATE_TABLE} && defined $self->{UPDATE_TABLE_ID_COLUMN} && defined $self->{UPDATE_TABLE_ID_VALUE}) {
    if (defined $self->{UPDATE_TABLE_STATUS_COLUMN} && defined $self->{UPDATE_TABLE_COMMENT_COLUMN}) {
      $sth = $dbh->prepare("UPDATE $self->{UPDATE_TABLE} set $self->{UPDATE_TABLE_STATUS_COLUMN}=?,$self->{UPDATE_TABLE_COMMENT_COLUMN}=? where $self->{UPDATE_TABLE_ID_COLUMN}=?") || die $dbh->errstr;
      $sth->execute
	(
	 $self->{UPDATE_TABLE_QUEUED_STATUS_VALUE},
	 "Queued in Task Manager",
	 $self->{UPDATE_TABLE_ID_VALUE}
	);
      $sth->finish();
    }
    elsif (defined $self->{UPDATE_TABLE_STATUS_COLUMN}) {
      $sth = $dbh->prepare("UPDATE $self->{UPDATE_TABLE} set $self->{UPDATE_TABLE_STATUS_COLUMN}=? where $self->{UPDATE_TABLE_ID_COLUMN}=?") || die $dbh->errstr;
      $sth->execute
	(
	 $self->{UPDATE_TABLE_QUEUED_STATUS_VALUE},
	 $self->{UPDATE_TABLE_ID_VALUE}
	);
      $sth->finish();
      
    }
    elsif ($self->{UPDATE_TABLE_COMMENT_COLUMN} ne "") {
      $sth = $dbh->prepare("UPDATE $self->{UPDATE_TABLE} set $self->{UPDATE_TABLE_COMMENT_COLUMN}=? where $self->{UPDATE_TABLE_ID_COLUMN}=?") || die $dbh->errstr;
      $sth->execute
	(
	 "Queued in Task Manager",
	 $self->{UPDATE_TABLE_ID_VALUE}
	);
      $sth->finish();
    }
  }


  $dbh->disconnect();

  return $self;

}


sub GetNextTask() {

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth2=$dbh->prepare("SELECT int_1 FROM sysvals WHERE sysval='HEAVYTASKS'");
  $sth2->execute();
  my $total_heavytasks_allowed=$sth2->fetchrow();
  $sth2->finish();
  $total_heavytasks_allowed=10 if !defined $total_heavytasks_allowed;


  # Find the next task with the highest priority that is in the QUEUED state.

  $dbh->do("LOCK TABLES tasks WRITE");
  my $sth = $dbh->prepare("SELECT taskid,command,arguments,timeout,priority,isheavy,notification_emails,update_table,update_table_id_column,update_table_id_value,update_table_status_column,update_table_queued_status_value,update_table_started_status_value,update_table_executed_status_value,update_table_error_status_value,update_table_timeout_status_value,update_table_cancelled_status_value,update_table_comment_column FROM tasks where status=\"QUEUED\" and (schedule is NULL or schedule<=NOW()) order by priority asc limit 1") || die $dbh->errstr;
  $sth->execute() || die $dbh->errstr;

  my ($taskid,$command,$arguments,$timeout,$priority,$isheavy,$notification_emails,$update_table,$update_table_id_column,$update_table_id_value,$update_table_status_column,$update_table_queued_status_value,$update_table_started_status_value,$update_table_executed_status_value,$update_table_error_status_value,$update_table_timeout_status_value,$update_table_cancelled_status_value,$update_table_comment_column) = $sth->fetchrow();
  $sth->finish();

  if (defined $taskid) {
      # Found one

      # Now let us see if we should use it or not. For the heavyweight ssh tasks, we only
      # want to run X at a time.
      if ($isheavy > 0) {

#	  Debug "Heavy Task: $command found!";

	  my $sth1=$dbh->prepare("SELECT count(*) FROM tasks WHERE isheavy>0 AND status='STARTED' AND addedon>NOW()-INTERVAL 10 MINUTE");
	  $sth1->execute() or die $dbh->errstr;

	  my $heavytasks = $sth1->fetchrow();

	  if ($heavytasks >= $total_heavytasks_allowed) {


#	      Debug "Concurrent heavy tasks running is $heavytasks. Allowed is $total_heavytasks_allowed. Grab a non-heavy one";

	      # Instead, grab a non-heavy task
	      my $sth = $dbh->prepare("SELECT taskid,command,arguments,timeout,priority,isheavy,notification_emails,update_table,update_table_id_column,update_table_id_value,update_table_status_column,update_table_queued_status_value,update_table_started_status_value,update_table_executed_status_value,update_table_error_status_value,update_table_timeout_status_value,update_table_cancelled_status_value,update_table_comment_column FROM tasks where status=\"QUEUED\" and isheavy=0 AND (schedule is NULL or schedule<=NOW()) order by priority asc limit 1") || die $dbh->errstr;
	      $sth->execute() || die $dbh->errstr;
	      
	      ($taskid,$command,$arguments,$timeout,$priority,$isheavy,$notification_emails,$update_table,$update_table_id_column,$update_table_id_value,$update_table_status_column,$update_table_queued_status_value,$update_table_started_status_value,$update_table_executed_status_value,$update_table_error_status_value,$update_table_timeout_status_value,$update_table_cancelled_status_value,$update_table_comment_column) = $sth->fetchrow();
	      $sth->finish();

	      # Nothing else to process...
	      if (!defined $taskid) {
		  $dbh->do("UNLOCK TABLES");
		  $dbh->disconnect();
		  return 0;
	      }


#	      Debug "should have successfully grabbed $command which is non-heavy. In other words, $isheavy should be 0" if defined $taskid;
	      
	  }

      }
      
  }
  else {
    $dbh->do("UNLOCK TABLES");
    $dbh->disconnect();
    return 0;
  }

  $dbh->do("UNLOCK TABLES");
  $dbh->disconnect();

  if (!defined $update_table_comment_column) {
    $update_table_comment_column="";
  }

#  Debug "returning a new task with taskid=$taskid";

  return new Edgewater::Taskmanager
    (TASKID=>$taskid,
     COMMAND=>$command,
     ARGUMENTS=>$arguments,
     PRIORITY=>$priority,
     ISHEAVY=>$isheavy,
     NOTIFICATION_EMAILS=>$notification_emails,
     TIMEOUT=>$timeout,
     UPDATE_TABLE=>$update_table,
     UPDATE_TABLE_ID_COLUMN=>$update_table_id_column,
     UPDATE_TABLE_ID_VALUE=>$update_table_id_value,
     UPDATE_TABLE_STATUS_COLUMN=>$update_table_status_column,
     UPDATE_TABLE_QUEUED_STATUS_VALUE=>$update_table_queued_status_value,
     UPDATE_TABLE_STARTED_STATUS_VALUE=>$update_table_started_status_value,
     UPDATE_TABLE_EXECUTED_STATUS_VALUE=>$update_table_executed_status_value,
     UPDATE_TABLE_ERROR_STATUS_VALUE=>$update_table_error_status_value,
     UPDATE_TABLE_TIMEOUT_STATUS_VALUE=>$update_table_timeout_status_value,
     UPDATE_TABLE_CANCELLED_STATUS_VALUE=>$update_table_cancelled_status_value,
     UPDATE_TABLE_COMMENT_COLUMN=>$update_table_comment_column,
    );
}

sub SetStatus($$) {
  my $self=shift;
  my $newstatus = shift;

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE tasks SET status=? WHERE taskid=$self->{TASKID}");
  $sth->execute($newstatus) || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

}

sub GetStatus($) {
  my $self=shift;

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("SELECT status FROM tasks WHERE taskid=$self->{TASKID}");
  $sth->execute() || die $dbh->errstr;
  
  my $status = $sth->fetchrow();

  $sth->finish();
 
  $dbh->disconnect();

  if (defined $status) {
    return $status;
  }
  else {
    return "UNKNOWN";
  }

}

sub SetArguments($$) {
  my $self=shift;
  my $newargs = shift;

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE tasks SET arguments=? WHERE taskid=$self->{TASKID}");
  $sth->execute($newargs) || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

}


sub SetUpdateTableTimeoutStatus($) {
  my $self=shift;

  if (!defined $self->{UPDATE_TABLE} ||
      !defined $self->{UPDATE_TABLE_STATUS_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_VALUE} ||
      !defined $self->{UPDATE_TABLE_TIMEOUT_STATUS_VALUE} ) {
    return;
  }


  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE $self->{UPDATE_TABLE} SET $self->{UPDATE_TABLE_STATUS_COLUMN}=? where $self->{UPDATE_TABLE_ID_COLUMN}=?");
  $sth->execute($self->{UPDATE_TABLE_TIMEOUT_STATUS_VALUE},$self->{UPDATE_TABLE_ID_VALUE}) || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

}

sub SetUpdateTableComment($$) {
  my $self=shift;
  my $comment=shift;

  # Make sure we use comment column
  if (!defined $self->{UPDATE_TABLE_COMMENT_COLUMN} || $self->{UPDATE_TABLE_COMMENT_COLUMN} eq "") {
    return $self;
  }

  if (!defined $self->{UPDATE_TABLE} ||
      !defined $self->{UPDATE_TABLE_ID_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_VALUE} ) {
    return $self;
  }


  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE $self->{UPDATE_TABLE} SET $self->{UPDATE_TABLE_COMMENT_COLUMN}=? where $self->{UPDATE_TABLE_ID_COLUMN}=?");
  $sth->execute($comment,$self->{UPDATE_TABLE_ID_VALUE}) || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

  return $self;

}


sub SetUpdateTableStartedStatus($) {
  my $self=shift;

  if (!defined $self->{UPDATE_TABLE} ||
      !defined $self->{UPDATE_TABLE_STATUS_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_VALUE} ||
      !defined $self->{UPDATE_TABLE_STARTED_STATUS_VALUE} ) {
    return $self;
  }

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE $self->{UPDATE_TABLE} SET $self->{UPDATE_TABLE_STATUS_COLUMN}=? where $self->{UPDATE_TABLE_ID_COLUMN}=?");
  $sth->execute($self->{UPDATE_TABLE_STARTED_STATUS_VALUE},$self->{UPDATE_TABLE_ID_VALUE}) || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

  return $self;
}

sub SetUpdateTableExecutedStatus($) {
  my $self=shift;

  if (!defined $self->{UPDATE_TABLE} ||
      !defined $self->{UPDATE_TABLE_STATUS_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_VALUE} ||
      !defined $self->{UPDATE_TABLE_EXECUTED_STATUS_VALUE} ) {
    return $self;
  }

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE $self->{UPDATE_TABLE} SET $self->{UPDATE_TABLE_STATUS_COLUMN}=? where $self->{UPDATE_TABLE_ID_COLUMN}=?");
  $sth->execute($self->{UPDATE_TABLE_EXECUTED_STATUS_VALUE},$self->{UPDATE_TABLE_ID_VALUE}) || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

  return $self;
}

sub SetUpdateTableQueuedStatus($) {
  my $self=shift;

  if (!defined $self->{UPDATE_TABLE} ||
      !defined $self->{UPDATE_TABLE_STATUS_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_VALUE} ||
      !defined $self->{UPDATE_TABLE_QUEUED_STATUS_VALUE} ) {
    return $self;
  }

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE $self->{UPDATE_TABLE} SET $self->{UPDATE_TABLE_STATUS_COLUMN}=? where $self->{UPDATE_TABLE_ID_COLUMN}=?");
  $sth->execute($self->{UPDATE_TABLE_QUEUED_STATUS_VALUE},$self->{UPDATE_TABLE_ID_VALUE}) || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

  return $self;
}

sub SetUpdateTableCancelledStatus($) {
  my $self=shift;

  if (!defined $self->{UPDATE_TABLE} ||
      !defined $self->{UPDATE_TABLE_STATUS_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_VALUE} ||
      !defined $self->{UPDATE_TABLE_CANCELLED_STATUS_VALUE} ) {
    return $self;
  }

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE $self->{UPDATE_TABLE} SET $self->{UPDATE_TABLE_STATUS_COLUMN}=? where $self->{UPDATE_TABLE_ID_COLUMN}=?");
  $sth->execute($self->{UPDATE_TABLE_CANCELLED_STATUS_VALUE},$self->{UPDATE_TABLE_ID_VALUE}) || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

  return $self;
}

sub SetErrorStatus($;$) {
  my $self=shift;
  my $notifymessage=shift;

  $self->SetStatus("FAILURE");

  $self->SetUpdateTableErrorStatus();

  if (!defined $notifymessage && $self->{NOTIFICATION_EMAILS} ne "") {
    $self->SendEmails("FAILURE","");
  }
  elsif ($self->{NOTIFICATION_EMAILS} ne "") {
    $self->SendEmails("FAILURE",$notifymessage);
  }


  return $self;
}

sub SetUpdateTableErrorStatus($) {
  my $self=shift;

  if (!defined $self->{UPDATE_TABLE} ||
      !defined $self->{UPDATE_TABLE_STATUS_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_COLUMN} ||
      !defined $self->{UPDATE_TABLE_ID_VALUE} ||
      !defined $self->{UPDATE_TABLE_ERROR_STATUS_VALUE} ) {
    return $self;
  }

  my $dbh=ewn_db::get_dbh(retry=>100);

  my $sth = $dbh->prepare("UPDATE $self->{UPDATE_TABLE} SET $self->{UPDATE_TABLE_STATUS_COLUMN}=? where $self->{UPDATE_TABLE_ID_COLUMN}=?");
  $sth->execute($self->{UPDATE_TABLE_ERROR_STATUS_VALUE},$self->{UPDATE_TABLE_ID_VALUE}) || die $dbh->errstr;
  $sth->finish();
 
  $dbh->disconnect();

  return $self;
}

sub SendEmails($$$) {
  my ($self,$status,$message)=@_;

  if ($self->{NOTIFICATION_EMAILS} eq "") {
    return $self;
  }

  my $subj;
  my $body;
  my $details="Thank you for using EdgeView, your VoIP troubleshooting and monitor system!";

  if ($message ne "") {
    my $m="";
    if ($message =~ /^(.*)$/) {
      $m = $1;
    }
    else {
	$m = $message;
    }
    
    if ($m ne "") {
      $details="Details: $m\n\n".$details;
    }
    else {
      $details = "No details available.\n\n".$details;
    }
  }

  my $id;
  if ($self->{TASKID} =~ /^(\d+)$/) {
    $id = $1;
  }

  my $thedate=`date`;
  if ($status eq "EXECUTED") {
    $subj = "EdgeView System Notification: Task Executed Successfully.";
    $body = "EdgeView System is notifying you that a task executed successfully at $thedate.\n\n$details";
  }
  elsif ($status =~ /^FAILURE/) {
    $subj = "EdgeView System Notification: Task Failed to Execute.";
    $body = "EdgeView System is notifying you that a task (id=$id) failed at $thedate.\n\n$details";
  }
  elsif ($status eq "TIMEOUT") {
    $subj = "EdgeView System Notification: Task Failed to Execute.";
    $body = "EdgeView System is notifying you that a task (id=$id) failed at $thedate due to a Timeout error.";
  }
  elsif ($status eq "CANCELLED") {
    $subj = "EdgeView System Notification: Task Cancelled.";
    $body = "EdgeView System is notifying you that a task (id=$id) was cancelled at $thedate.\n\n$details";
  }

  my @emails=split(/,/,$self->{NOTIFICATION_EMAILS});

  foreach my $email (@emails) {
      next if length($email) < 4;

      my $tmpsubj=""; 
      my $tmpbody="";
      my $tmpemail="";
      
      # Get rid of dependency error
      if ($email =~ /^(.*)$/) {
	  $tmpemail = $1;
      }

      if ($subj =~ /^(.*)$/) {
	  $tmpsubj = $1;
      }

      if ($body =~ /^(.*)$/) {
	  $tmpbody = $1;
      }


      Edgewater::Mail::Send($tmpsubj,$tmpbody,$tmpemail,"support");
      ewn_notify::AddAlertHistory(0,0,"tasknotify",$email,"","",$subj,$body);
  }

  
}

sub SetCompleteStatus($;$) {
  my $self=shift;
  my $notifymessage=shift;

  $self->SetStatus("EXECUTED");

  $self->SetUpdateTableExecutedStatus();

  if (!defined $notifymessage && $self->{NOTIFICATION_EMAILS} ne "") {
    $self->SendEmails("EXECUTED","");
  }
  elsif ($self->{NOTIFICATION_EMAILS} ne "") {
    $self->SendEmails("EXECUTED",$notifymessage);
  }

  return $self;
}

sub CleanupOldTasks {

  # Get rid of all the non-QUEUED tasks over 7 days old
  my $dbh = ewn_db::get_dbh(retry=>100);

  $dbh->do("DELETE from tasks where addedon < DATE_SUB(NOW(),INTERVAL 7 day) AND status<>\"QUEUED\"") || die $dbh->errstr;

  $dbh->disconnect();

}  

sub WaitForTaskComplete {
  my $self=shift;
  my $seconds=shift;

  if (!defined $seconds) {
    $seconds = 300;
  }

  my $dbh=ewn_db::get_dbh(retry=>100);
 
  my $sth;
  my $returncode;
  my $done=0;

  if (!defined $self->{TASKID}) {
    die "Task ID not found. Was AddTaskToQueue called?";
  }

  my $timespent = 0;


  while (!$done) {
    $sth = $dbh->prepare("SELECT status FROM tasks WHERE taskid=$self->{TASKID}");
    $sth->execute() || die $dbh->errstr;

    my $status = $sth->fetchrow();
    if (!defined $status) {
      $returncode = 0;
      $done = 1;
    }
    if ($status =~ /^FAILURE/) {
      $returncode = 99;
      $done = 1;
    }
    elsif ($status eq "TIMEOUT") {
      $returncode = 98;
      $done = 1;
    }
    elsif ($status eq "CANCELLED") {
      $returncode = 97;
      $done = 1;
    }
    elsif ($status eq "EXECUTED") {
      $returncode = 100;
      $done = 1;
    }
    elsif ($status eq "QUEUED" || $status eq "SPAWNED" || $status eq "STARTED") {

      if ($timespent == $seconds) {
	# At the edge, try for one more second for a race condition
	sleep 1;
	$timespent += 1;
      }
      elsif ($timespent > $seconds) {
	$done = 1;
	$returncode = 98;
      }
      else {
	sleep 5;
	$timespent += 5;
      }
    } 
    else {
      # Undefined, need to add it
      die "Undefined status in Edgewater::Taskmanager::WaitForTaskComplete: $status";

    }

    $sth->finish();

  }

  $dbh->disconnect();

  return $returncode;

}

sub WaitForTaskStart($$) {
  my $self=shift;
  my $seconds=shift;

  if (!defined $seconds) {
    $seconds = 300;
  }

  my $dbh=ewn_db::get_dbh(retry=>100);
 
  my $sth;
  my $returncode;
  my $done=0;

  my $timespent=0;

  while (!$done && $timespent < $seconds) {
    $sth = $dbh->prepare("SELECT status FROM tasks WHERE taskid=$self->{TASKID}");
    $sth->execute() || die $dbh->errstr;

    my $status = $sth->fetchrow();
    if (!defined $status) {
      $returncode = 0;
      $done = 1;
    }
    if ($status =~ /^FAILURE/) {
      $returncode = 99;
      $done = 1;
    }
    elsif ($status eq "TIMEOUT") {
      $returncode = 98;
      $done = 1;
    }
    elsif ($status eq "CANCELLED") {
      $returncode = 97;
      $done = 1;
    }
    elsif ($status eq "SPAWNED" || $status eq "STARTED") {
      $returncode = 2;
      $done = 1;
    }
    elsif ($status eq "EXECUTED") {
      $returncode = 100;
      $done = 1;
    }
    elsif ($status eq "QUEUED") {
      sleep 5;
      $timespent += 5;
    } 
    else {
      # Undefined, need to add it
      die "Undefined status in Edgewater::Taskmanager::WaitForTaskComplete: $status";

    }

    $sth->finish();

  }

  $dbh->disconnect();

  if (!defined $returncode && $timespent >= $seconds) {
    $returncode = 98; # Timeout
  }

  return $returncode;

}

########################################################
# END
########################################################

=head1 METHODS

new - Creates the task object. Caller should create the object with a COMMAND which is the full path to a system-executable script or UNIX command. 
  Optional parameters include:
    - ARGUMENTS: space-separated list of the arguments to pass to the COMMAND
    - TIMEOUT: number of seconds to wait for the COMMAND to execute. Default is 10.
    - PRIORITY: Any signed integer value, where tasks are executed with the lower number first. In other words a -2 priority executes before a priority 0. Default is 0.
    - ISHEAVY: If > 0, indicates a heavy task. We limit the number of heavy tasks that can run simultaneously. Examples of heavy tasks are those that use SSH to log into an EdgeMarc.
    - UPDATE_TABLE: If specified, then this is the MySQL table name that can be updated with the UpdateTable* methods. Note that UPDATE_TABLE_ID_COLUMN, UPDATE_TABLE_ID_VALUE and UPDATE_TABLE_STATUS_COLUMN should be specified as well.
    - UPDATE_TABLE_ID_COLUMN: Column name used to locate a row in the user's UPDATE_TABLE.
    - UPDATE_TABLE_ID_VALUE: Value to match with the UPDATE_TABLE_ID_COLUMN
    - UPDATE_TABLE_STATUS_COLUMN: Column name of the status column that will be updated with values specified by UPDATE_TABLE_QUEUED_STATUS_VALUE, UPDATE_TABLE_CANCELLED_STATUS_VALUE, UPDATE_TABLE_STARTED_STATUS_VALUE,UPDATE_TABLE_EXECUTED_STATUS_VALUE, UPDATE_TABLE_ERROR_STATUS_VALUE, or UPDATE_TABLE_TIMEOUT_STATUS_VALUE when the QUEUED, STARTED, EXECUTED, ERROR, CANCELLED or TIMEOUT state occurs.
    - UPDATE_TABLE_QUEUED_STATUS_VALUE - Any string or integer value that will be used to set the UPDATE_TABLE_STATUS_COLUMN when the task is first queued with AddTaskToQueue.
    - UPDATE_TABLE_STARTED_STATUS_VALUE - Any string or integer value that will be used to set the UPDATE_TABLE_STATUS_COLUMN when the task is started.
    - UPDATE_TABLE_EXECUTED_STATUS_VALUE - Any string or integer value that will be used to set the UPDATE_TABLE_STATUS_COLUMN when the task is completes execution.
    - UPDATE_TABLE_ERROR_STATUS_VALUE - Any string or integer value that will be used to set the UPDATE_TABLE_STATUS_COLUMN when the task has an non-zero error exit code returned from execution.
    - UPDATE_TABLE_TIMEOUT_STATUS_VALUE - Any string or integer value that will be used to set the UPDATE_TABLE_STATUS_COLUMN when the task fails to complete before the TIMEOUT value in number of seconds.
    - UPDATE_TABLE_CANCELLED_STATUS_VALUE - Any string or integer value that will be used to set the UPDATE_TABLE_STATUS_COLUMN when the task is cancelled with CancelTask.
    - UPDATE_TABLE_COMMENT_COLUMN - Column name of a "comment" column in the UPDATE_TABLE if any. It will be used when SetUpdateTableComment is called.


AddTaskToQueue - This method inserts the given task onto the task queue, which is really just an insert into the tasks table. If a "schedule" argument is specified (in MySQL timestamp format), then it will include the schedule of when to exectute the task. To execute immediately, specify an empty string for the schedule.

LookupTask() - Given the integer task ID, it will create a task object from the matching ID in the task table.

LookupTaskIDByUpdateID() - Given the Update Table, the Update Table ID Column and the Update Table ID Value, it will create a task object from the latest matching Task ID in the task table.

RequeueTask() - This method will reset the status of the task to QUEUED so that it will execute again at the appropriate scheduled time, if set, and priority. 

RequeueTaskLater() - This method will reset the status of the task to QUEUED so that it will execute again at the current time in the database + the number of minutes specified in the argument. 

GetSchedule() - Returns the current schedule value or undefined if not set. Note that the schedule can change from the original schedule set if RequeueTask or RequeueTaskLater is called.

CancelTask() - This method will cancel the task by setting the status to CANCELLLED so that it will not be executed. 

CancelTaskWithoutNotify() - This method will cancel the task by setting the status to CANCELLLED so that it will not be executed. It will NOT send emails to the NOTIFICATION_EMAILS for the task.

WaitForTaskStart() - Optional timeout value in seconds may be specified. Default timeout is 300 seconds. This method polls the status column of the tasks table to see when the status indicates that the task is started or spawned (generally the same thing, it spawns and then immediately starts). Possible return codes are 100: normal completion, 99: Error, 98: Timeout, 97: Cancelled, or 0: cannot find the task in the database.

WaitForTaskComplete() - Optional timeout value in seconds may be specified. Default timeout is 300 seconds. This method polls the status column of the tasks table to see when the status indicates that the task is completed. Possible return codes are 100: normal completion, 99: Error, 98: Timeout, 97: Cancelled, or 0: cannot find the task in the database. 

SetStatus() - Used by the taskengine daemon, this method sets the tasks table status to the specified status value, either "QUEUED","SPAWNED","STARTED","EXECUTED","FAILURE:exitcode", "TIMEOUT" or "CANCELLED".

GetStatus() - Returns the status value from the tasks table as a string.

GetNextTask() - Used by the taskengine daemon, this method gets the Next Task in priority order with the status "QUEUED". If the task is scheduled, then it will get the next task only if the scheduled datetime is passed.

SetArguments() - Changes the arguments currently in the task to the new argument values.

SetUpdateTableComment() - Used by taskengine, if an Update Table was specified, then this method will set the comment column of that table to the specified string comment value.

SetUpdateTableQueuedStatus() - Used by taskengine, if an Update Table was specified, then this method will set the status column of that table to the queued value.

SetUpdateTableStartedStatus() - If an Update Table was specified, then this method will set the status column of that table to the "SPAWNED" or "STARTED" value.

SetUpdateTableExecutedStatus() - Used by taskengine, if an Update Table was specified, then this method will set the status column of that table to the "EXECUTED" value.

SetUpdateTableTimeoutStatus() - Used by task engine, if an Update Table was specified, then this method will set the status column of that table to the timeout code.

SetUpdateTableCancelledStatus() - If an Update Table was specified, then this method will set the status column of that table to the cancelled code.

SetUpdateTableErrorStatus() - Used by task engine, if an Update Table was specified, then this method will set the status column of that table to the error code.

SetErrorStatus() - Updates the tasks table and the UPDATE_TABLE with the appropriate error status values. 

SetCompleteStatus() - Updates the tasks table and the UPDATE_TABLE with the appropriate success status values. 

CleanupOldTasks() - Class Method. Removes all the tasks older than two days if they are not in the QUEUED state. Scheduled tasks remain in the QUEUED state until they are started.

=cut

1;
