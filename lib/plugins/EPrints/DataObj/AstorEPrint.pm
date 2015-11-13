package EPrints::DataObj::AstorEPrint;

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
	return "astor_eprint";
}

sub parent
{
	  my ($self) = @_;

	  my $eprintid = $self->value('eprintid');
	  return if !$eprintid;

	  return $self->{session}->dataset('eprint')->dataobj($eprintid);
}

sub remove
{
	  my ($self) = @_;

	  my $eprint = $self->parent;
	  if (defined $eprint)
	  {
		  $eprint->set_value('archive_status', undef);
		  $eprint->commit;
	  }

	  return $self->SUPER::remove();
}

1;
