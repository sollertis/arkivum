=head1 NAME

EPrints::Plugin::Screen::EPrint::AStorRestore

=cut

package EPrints::Plugin::Screen::EPrint::AStorRestore;

our @ISA = ( 'EPrints::Plugin::Screen::EPrint' );

use strict;

sub new
{
	my( $class, %params ) = @_;

	my $self = $class->SUPER::new(%params);

	$self->{appears} = [
		{
			place => "eprint_actions",
			position => 800,
		},
	];

	$self->{actions} = [qw/ send cancel /];

	return $self;
}


sub can_be_viewed
{
	my( $self ) = @_;

	return 0 if !$self->SUPER::can_be_viewed;

  # Get the eprint and check the archive_status
	my $eprint = $self->{processor}->{eprint};

  my $archive_status = $eprint->get_value("archive_status");

  return 0 if not defined $archive_status;

  return 0 if $archive_status ne "archived";

	return 1;

}

sub allow_send
{
	my( $self ) = @_;

	return $self->can_be_viewed;
}

sub allow_cancel
{
	my( $self ) = @_;

	return 1;
}

sub action_cancel
{
	my( $self ) = @_;

	$self->{processor}->{screenid} = "EPrint::View";
}



sub render
{
	my( $self ) = @_;

	my $eprint = $self->{processor}->{eprint};

	my $page = $self->{session}->make_doc_fragment();

  my $time_to_restore = $self->_calculate_restore_time();

  if ( defined $time_to_restore ) {
    $time_to_restore = "is " . $time_to_restore;
  }
  else
  {
      $time_to_restore = " could not be estimated at this time.";
  }
	
	$page->appendChild( $self->html_phrase("intro",
		eprint => $eprint->render_citation_link,
		restoretime => $self->{session}->make_text( $time_to_restore),
	) );
  
	my $form = $self->render_form;
	
	$page->appendChild( $form );
	
	my $reason = $self->{session}->make_doc_fragment;
	my $reason_static = $self->{session}->make_element( "div", id=>"ep_mail_reason_fixed",class=>"ep_only_js" );
	$reason_static->appendChild( $self->html_phrase( "reason" ) );
	$reason_static->appendChild( $self->{session}->make_text( " " ));	
	
	my $edit_link = $self->{session}->make_element( "a", href=>"#", onclick => "EPJS_blur(event); EPJS_toggle('ep_mail_reason_fixed',true,'block');EPJS_toggle('ep_mail_reason_edit',false,'block');\$('ep_mail_reason_edit').focus(); \$('ep_mail_reason_edit').select(); return false", );
	$reason_static->appendChild( $self->{session}->html_phrase( "mail_edit_click",
		edit_link => $edit_link ) ); 
	$reason->appendChild( $reason_static );
	

	my $div = $self->{session}->make_element( "div", class => "ep_form_field_input" );

	my $textarea = $self->{session}->make_element(
		"textarea",
		id => "ep_mail_reason_edit",
		class => "ep_no_js",
		name => "reason",
		rows => 5,
		cols => 60,
		wrap => "virtual" );
	$textarea->appendChild( $self->html_phrase( "reason" ) ); 
	$reason->appendChild( $textarea );

	# remove any markup:
	my $title = $self->{session}->make_text( 
		EPrints::Utils::tree_to_utf8( 
			$eprint->render_description() ) );
	
	my $from_user =$self->{session}->current_user;
	
	my $content = $self->html_phrase(
		"mail",
		user => $from_user->render_description,
		email => $self->{session}->make_text( $from_user->get_value( "email" )),
		citation => $self->{processor}->{eprint}->render_citation,
		url => $self->{session}->render_link(
				$self->{processor}->{eprint}->get_control_url ),
		reason => $reason );

	my $body = $self->{session}->html_phrase(
		"mail_body",
		content => $content );

	my $subject = $self->html_phrase( "subject" );

	my $view = $self->{session}->html_phrase(
		"mail_view",
		subject => $subject,
		to => $self->{session}->html_phrase( "archive_name" ),
		from => $from_user->render_description,
		body => $body );

	$div->appendChild( $view );
	
	$form->appendChild( $div );

	$form->appendChild( $self->{session}->render_action_buttons(
		_class => "ep_form_button_bar",
		"send" => $self->phrase( "action:send:title" ),
		"cancel" => $self->phrase( "action:cancel:title" ),
 	) );

	return( $page );
}	


sub action_send
{
	my( $self ) = @_;

	my $eprint = $self->{processor}->{eprint};
	my $user = $eprint->get_user();
  my $repository = $self->{session}->get_repository();


	my $reason = $self->{session}->param( "reason" );

  # Create a request to restore the specified eprint
  $repository->dataset( "astor_eprint" )->create_dataobj({
	  eprintid => $eprint->get_value("eprintid"),
	  userid => $user->get_value("userid"),
	  justification => $reason,
	  astor_status => 'restore_requested',
  });

	$self->{processor}->add_message( "message",
		$self->{session}->html_phrase( "Plugin/Screen/EPrint/AStorRestore:RequestOK" ) );

  my $subject = $self->html_phrase( "subject" );

  # Construct a history event for the restore request
	my $history_ds = $self->{session}->get_repository->get_dataset( "history" );

	my %hitem;

	my $from_user = $self->{session}->current_user;
	
	$reason = $self->{session}->make_text( $self->{session}->param( "reason" ) );
	$hitem{message} = $self->html_phrase(
	  "mail",
	  user => $from_user->render_description,
	  email => $self->{session}->make_text( $from_user->get_value( "email" )),
	  citation => $self->{processor}->{eprint}->render_citation,
	  url => $self->{session}->render_link($self->{processor}->{eprint}->get_control_url ),
	  reason => $reason 
	);

	$history_ds->create_object( 
		$self->{session},
		{
			userid=>$user->get_value("userid"),
			datasetid=>"eprint",
			objectid=>$eprint->get_id,
			revision=>$eprint->get_value( "rev_number" ),
			action=>"note",
			details=> EPrints::Utils::tree_to_utf8( $hitem{message} , 80 ),
		}
	);

  # Return to the view page
	$self->{processor}->{screenid} = "EPrint::View";

}


sub _calculate_restore_time
{
	  my( $self ) = @_;

	  my $restore_time;
	  my $total_size = 0;

	  my $eprint = $self->{processor}->{eprint};

    if ( not defined $eprint ) {
        return $restore_time;
    }
    
    # Get the factor_a and factor_b magic numbers
    # for the calcuation.
  	my $factor_a = $self->param( "factor_a" );

    if ( not defined $factor_a ) {
        return $restore_time;
    }

  	my $factor_b = $self->param( "factor_b" );

    if ( not defined $factor_b ) {
        return $restore_time;
    }

    foreach my $doc ($eprint->get_all_documents())
    {
        foreach my $file (@{$doc->get_value( "files" )})
        {
	          my $filesize = $file->value( "filesize" );
	          $total_size = $total_size + $filesize;
        }
    }
    
    # Check we have a size and return if we do not
    if ( $total_size == 0 ) {
        return $restore_time;
    }
    
    # Convert the total size from bytes to megabytes
    $total_size = $total_size / (1024 * 1024);
        
    # restore time is total size * factor_a + factor_b    
    my $seconds = int(( $total_size * $factor_a ) + $factor_b);
    
    my $days = int($seconds/(24*60*60));
    if ( $days > 0 ) {
        $restore_time = $restore_time . "$days days, ";
    }
    
    my $hours = ($seconds/(60*60))%24;
    if ( $hours > 0 ) {
        $restore_time = $restore_time . "$hours hours, ";
    }
    
    my $mins = ($seconds/60)%60;
    if ( $mins > 0 ) {
        $restore_time = $restore_time . "$mins mins and ";
    }
    
    my $secs = $seconds%60;
    if ( $secs > 0 ) {
        $restore_time = $restore_time . "$secs secs";
    }
	  
    # return time in seconds for restore
    return $restore_time;
}

1;

=head1 COPYRIGHT

=for COPYRIGHT BEGIN

Copyright 2000-2011 University of Southampton.

=for COPYRIGHT END

=for LICENSE BEGIN

This file is part of EPrints L<http://www.eprints.org/>.

EPrints is free software: you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

EPrints is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
License for more details.

You should have received a copy of the GNU Lesser General Public
License along with EPrints.  If not, see L<http://www.gnu.org/licenses/>.

=for LICENSE END

