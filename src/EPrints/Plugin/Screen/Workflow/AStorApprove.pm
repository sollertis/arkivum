package EPrints::Plugin::Screen::Workflow::AStorApprove;

use base qw( EPrints::Plugin::Screen::Workflow );

use strict;

sub new
{
	my ($class, %params) = @_;

	my $self = $class->SUPER::new(%params);

	$self->{icon} = 'action_astor.png';

	$self->{actions} = [qw( cancel archive delete )];

	$self->{appears} = [
		{
			place => 'astor_item_actions',
			position => 50,
		},
		{
			place => 'astor_view_actions',
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

	my $doc = $astor->parent;
	return 0 if !defined $doc;

	return 0 if !$doc->is_set('archive_status');

	my $archive_status = $doc->value('archive_status');

	return 1 if $archive_status eq 'archive_requested' || $archive_status eq 'delete_requested';
}

sub render
{
	my ($self) = @_;

	my $repo = $self->repository;
	my $xml = $repo->xml;
	my $xhtml = $repo->xhtml;

	my $astor = $self->{processor}{dataobj};
	my $doc = $astor->parent;
	my $eprint = $doc->parent;

	my $frag = $xml->create_document_fragment;

	my $state = {
			'archive_requested' => 'archive',
			'delete_requested' => 'delete',
	}->{$doc->value('archive_status')};

	$frag->appendChild( $self->html_phrase("confirm_$state",
		astor => $astor->render_citation,
		document => $doc->render_citation_link,
		eprint => $eprint->render_citation_link,
	) );

	$frag->appendChild(my $form = $self->render_form);
	$form->appendChild($xml->create_data_element(
		'div',
		$repo->render_action_buttons(
			cancel => $repo->phrase( "lib/submissionform:action_cancel" ),
			$state => $self->phrase( "action:$state:title" ),
			_order => [ $state, "cancel" ]
		),
		class => 'ep_block'
	));

	return $frag;
}

sub allow_cancel { 1 }
sub allow_archive { shift->can_be_viewed }
sub allow_delete { shift->can_be_viewed }

sub action_cancel { $_[0]->{processor}{screenid} = $_[0]->view_screen }

sub action_archive
{
	my ($self) = @_;

	my $repo = $self->repository;

	my $astor = $self->{processor}{dataobj};
	my $doc = $astor->parent;

	if ($doc->value('archive_status') eq 'archive_requested')
	{
		$doc->set_value('archive_status', 'archive_approved');
		$doc->commit;
		
		$astor->set_value('astor_status', 'archive_scheduled');
		$astor->commit;
	}

	$self->action_cancel;
}

sub action_delete
{
	my ($self) = @_;

	my $repo = $self->repository;

	my $astor = $self->{processor}{dataobj};
	my $doc = $astor->parent;

	if ($doc->value('archive_status') eq 'delete_requested')
	{
		$doc->set_value('archive_status', 'delete_approved');
		$doc->commit;

		$astor->set_value('astor_status', 'delete_scheduled');
		$astor->commit;
	}

	$self->action_cancel;
}

1;
