package EPrints::DataObj::Astor;

our @ISA = qw( EPrints::DataObj::SubObject );

# The new method can simply return the constructor of the super class (Dataset)
sub new
{
	return shift->SUPER::new( @_ );
}

# This method is required to just return the dataset_id.
sub get_dataset_id
{
	my ($self) = @_;
	return "astor";
}

sub parent
{
	  my ($self) = @_;

	  my $docid = $self->value('docid');
	  return if !$docid;

	  return $self->{session}->dataset('document')->dataobj($docid);
}

sub remove
{
	  my ($self) = @_;

	  my $doc = $self->parent;
	  if (defined $doc)
	  {
		  $doc->set_value('archive_status', undef);
		  $doc->commit;
	  }

	  return $self->SUPER::remove();
}

1;
