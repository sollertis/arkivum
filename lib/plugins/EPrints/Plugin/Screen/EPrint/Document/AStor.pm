=head1 NAME

EPrints::Plugin::Screen::EPrint::Document::AStor

=head1 DESCRIPTION

Request escrow storage for documents.

=cut

package EPrints::Plugin::Screen::EPrint::Document::AStor;

our @ISA = ( 'EPrints::Plugin::Screen::EPrint::Document' );

use strict;

sub new
{
	my( $class, %params ) = @_;

	my $self = $class->SUPER::new(%params);

	$self->{icon} = "action_astor.png";

	$self->{appears} = [
		{
			place => "document_item_actions",
			position => 1600,
		},
	];
	
	$self->{actions} = [qw/ cancel create delete /];

	$self->{ajax} = "interactive";

	return $self;
}

sub allow_cancel { 1 }
sub allow_create { shift->can_be_viewed( @_ ) }
sub allow_delete { shift->can_be_viewed( @_ ) }

sub current_astor
{
	my ($self) = @_;

	return $self->repository->dataset('astor')->search(filters => [
		{ meta_fields => [qw( docid )], value => $self->{processor}->{document}->id, },
	])->item(0);
}

sub can_be_viewed
{
	my( $self ) = @_;

	my $doc = $self->{processor}->{document};
	return 0 if !$doc;

	return 0 if !$self->SUPER::can_be_viewed;

	return 1;
}

sub json
{
	my( $self ) = @_;

	my $json = $self->SUPER::json;
	return $json if !$self->{processor}->{refresh};

	for(@{$json->{documents}})
	{
		$_->{refresh} = 1, last
			if $_->{id} == $self->{processor}->{document}->id;
	}

	return $json;
}

sub render
{
	my ($self) = @_;

	my $repo = $self->repository;
	my $xml = $repo->xml;
	my $xhtml = $repo->xhtml;

	my $doc = $self->{processor}->{document};

	my $frag = $xml->create_document_fragment;

	my $astor = $self->current_astor;

	$frag->appendChild( $xml->create_data_element(
		'div',
		$self->html_phrase('help'),
		class => 'ep_block',
	) );

	$frag->appendChild( $xml->create_data_element(
		'div',
		$doc->render_value('archive_status'),
		class => 'ep_block',
	) );

	$frag->appendChild( $xml->create_data_element(
		'div',
		$self->render_file_list,
		class => 'ep_block',
	) );

	if (defined $astor)
	{
		$frag->appendChild($self->render_control($astor));
	}
	else
	{
		$frag->appendChild($self->render_create);
	}

	return $frag;
}

sub render_file_list
{
	my ($self) = @_;

	my $repo = $self->repository;
	my $xml = $repo->xml;
	my $xhtml = $repo->xhtml;

	my $doc = $self->{processor}->{document};

	my $frag = $xml->create_document_fragment;

	$frag->appendChild(my $table = $xml->create_element('table'));

	foreach my $file (@{$doc->value('files')})
	{
		my @cells;
		foreach my $copy (@{$file->value('copies')})
		{
			push @cells, $xml->create_text_node($copy->{pluginid});
		}
		$table->appendChild($repo->render_row(
			$file->render_value('filename'),
			@cells,
		));
	}

	return $frag;
}

sub render_create
{
	my ($self) = @_;

	my $repo = $self->repository;
	my $xml = $repo->xml;
	my $xhtml = $repo->xhtml;

	my $dataset = $repo->dataset('astor');

	my $frag = $xml->create_document_fragment;

	my $form = $frag->appendChild($self->render_form);

	$form->appendChild(my $table = $xml->create_element('table'));

	foreach my $field ($dataset->field('justification'))
	{
		$table->appendChild($repo->render_row(
			$field->render_name($repo),
			$field->render_input_field($repo),
		));
	}

	$form->appendChild($repo->render_action_buttons(
		cancel => $repo->phrase('lib/submissionform:action_cancel'),
		create => $repo->phrase('lib/submissionform:action_create'),
		_order => [qw( create cancel )],
	));

	return $frag;
}

sub render_control
{
	my ($self, $astor) = @_;

	my $repo = $self->repository;
	my $xml = $repo->xml;
	my $xhtml = $repo->xhtml;

	my $doc = $self->{processor}->{document};

	my $dataset = $repo->dataset('astor');

	my $frag = $xml->create_document_fragment;

	$frag->appendChild($xml->create_data_element(
		'div',
		$astor->render_citation,
		class => 'ep_block'
	));

	my $form = $frag->appendChild($self->render_form);

	$form->appendChild(my $table = $xml->create_element('table'));

	if ($doc->value('archive_status') eq 'archived')
	{
		foreach my $field ($dataset->field('justification'))
		{
			$table->appendChild($repo->render_row(
				$field->render_name($repo),
				$field->render_input_field($repo),
			));
		}

		$form->appendChild($repo->render_action_buttons(
			delete => $repo->phrase('lib/submissionform:action_delete'),
			cancel => $repo->phrase('lib/submissionform:action_cancel'),
			_order => [qw( delete cancel )],
		));
	}
	else
	{
		$form->appendChild($repo->render_action_buttons(
			cancel => $repo->phrase('lib/submissionform:action_cancel'),
			_order => [qw( cancel )],
		));
	}

	return $frag;
}

sub action_create
{
	my( $self ) = @_;

	my $repo = $self->repository;
	my $dataset = $repo->dataset('astor');

	$self->{processor}->{redirect} = $self->{processor}->{return_to}
		if !$self->wishes_to_export;

	my $doc = $self->{processor}->{document};

	my $epdata = {
		docid => $doc->id,
		userid => $repo->current_user->id,
	};

	foreach my $field ($dataset->field('justification'))
	{
		$epdata->{$field->name} = $field->form_value($repo);
	}

	my $astor = $dataset->create_dataobj($epdata);

	$doc->set_value('archive_status', 'archive_requested');
	$doc->commit;
}

sub action_delete
{
	my( $self ) = @_;

	my $repo = $self->repository;
	my $dataset = $repo->dataset('astor');
	my $astor = $self->current_astor;

	$self->{processor}->{redirect} = $self->{processor}->{return_to}
		if !$self->wishes_to_export;

	my $doc = $self->{processor}->{document};

	foreach my $field ($dataset->field('justification'))
	{
		$astor->set_value($field->name, $field->form_value($repo));
	}

	$astor->commit;

	$doc->set_value('archive_status', 'delete_requested');
	$doc->commit;
}

1;

=head1 COPYRIGHT

=for COPYRIGHT BEGIN

Copyright 2000-2013 University of Southampton.

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

