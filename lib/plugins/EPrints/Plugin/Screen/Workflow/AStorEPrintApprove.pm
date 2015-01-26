package EPrints::Plugin::Screen::Workflow::AStorEPrintApprove;

use base qw( EPrints::Plugin::Screen::Workflow );

use strict;

sub new
{
	my ($class, %params) = @_;

	my $self = $class->SUPER::new(%params);

	$self->{icon} = 'action_astor.png';

	$self->{actions} = [qw( cancel approve )];

	$self->{appears} = [
		{
			place => 'astor_eprint_item_actions',
			position => 50,
		},
		{
			place => 'astor_eprint_view_actions',
			position => 50,
		},
	];

	return $self;
}

sub can_be_viewed
{
	my ($self) = @_;

	my $astor = $self->{processor}{dataobj};
	return 0 if !defined $astor;

	return 0 if !$astor->is_set('astor_status');

	my $astor_status = $astor->value('astor_status');

	return 1 if $astor_status eq 'restore_requested';

	return 1 if $astor_status eq 'delete_requested';
	
}

sub render
{
	my ($self) = @_;

	my $repo = $self->repository;
	my $xml = $repo->xml;
	my $xhtml = $repo->xhtml;

	my $astor = $self->{processor}{dataobj};
	my $eprint = $astor->parent;

	my $archive_status = $eprint->get_value("archive_status");
	my $astor_status = $astor->get_value("astor_status");
  my $userid  = $astor->get_value("userid");
  
  my $user = EPrints::DataObj::User->new( $repo, $userid );

	my $frag = $xml->create_document_fragment;

  my $action_phrase;
  
  if ($astor_status eq "restore_requested") 
  {
    $action_phrase = $self->html_phrase("confirm_restore_request",
		  user => $user->render_description,
		  email => $self->{session}->make_text( $user->get_value( "email" )),
		  citation => $eprint->render_citation,
	  );
  }
  else {
    if ($archive_status eq "archived") {
      $action_phrase = $self->html_phrase("confirm_delete_arkivum",
		    user => $user->render_description,
		    email => $self->{session}->make_text( $user->get_value( "email" )),
		    citation => $eprint->render_citation,
	    );
    }
    else {
      $action_phrase = $self->html_phrase("confirm_delete_request",
		    user => $user->render_description,
		    email => $self->{session}->make_text( $user->get_value( "email" )),
		    citation => $eprint->render_citation,
	    );
    }
  }

	$frag->appendChild( $action_phrase );

	$frag->appendChild(my $form = $self->render_form);
	$form->appendChild($xml->create_data_element(
		'div',
		$repo->render_action_buttons(
			cancel => $repo->phrase( "lib/submissionform:action_cancel" ),
			approve => $self->phrase( "action:approve:title" ),
			_order => [ "approve", "cancel" ]
		),
		class => 'ep_block'
	));

	return $frag;
}

sub allow_cancel { 1 }

sub allow_approve 
{
	my ($self) = @_;

	my $repo = $self->repository;

	my $astor = $self->{processor}{dataobj};
  
  my $eprint = $repo->eprint($astor->get_value("eprintid"));
  if (not defined $eprint) {
    return 0;
  }

	my $archive_status = $eprint->get_value("archive_status");

	my $astor_status = $astor->get_value("astor_status");
	
  return 1 if $astor_status eq "restore_requested";
  
  return 0 if $archive_status eq "archived";

  return 1;
}

sub action_cancel { $_[0]->{processor}{screenid} = $_[0]->view_screen }

sub action_approve
{
	my ($self) = @_;

	if ( $self->allow_approve() == 0) {
	  $self->action_cancel();
	  return;
	}

	my $repo = $self->repository;

	my $astor = $self->{processor}{dataobj};
  my $status;
  
	if ($astor->value('astor_status') eq 'restore_requested') {
	  $status = "restore_scheduled";
	}
	else {
	  $status = "delete_scheduled";
	}

	$astor->set_value('astor_status', $status);
	$astor->commit;
	$self->update_eprint_history();

	$self->action_cancel;
}

sub update_eprint_history
{
	my ($self) = @_;

	my $repo = $self->repository;

	my $astor = $self->{processor}{dataobj};
  
  my $eprint = $repo->eprint($astor->get_value("eprintid"));
  if (not defined $eprint) {
    return;
  }
  
  # Construct a history event for the approve request
	my $history_ds = $self->{session}->get_repository->get_dataset( "history" );

	my %hitem;

	my $user = $self->{session}->current_user;

	if ($astor->value('astor_status') eq 'restore_requested') {
	  $hitem{message} = $self->html_phrase(
	    "restore_approved_history",
	    user => $user->render_description,
	    email => $self->{session}->make_text( $user->get_value( "email" )),
	    citation => $eprint->render_citation
	  );
	}
	else {
	  $hitem{message} = $self->html_phrase(
	    "delete_approved_history",
	    user => $user->render_description,
	    email => $self->{session}->make_text( $user->get_value( "email" )),
	    citation => $eprint->render_citation
	  );
	}
	
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

}

1;
