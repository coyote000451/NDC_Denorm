#! c:\perl\bin

use strict;
use warnings;
use diagnostics;
use DBI;

# Notes
# This does NOT create an output file so you will need to run standard out to a file with a redirection ">" NDC_DENORM_<VERSION> -> NDC_DENORM_v210
# 

# Variables

my $table 	= 	"ndc_denorm ";
my $SQLSRV	= 	"IPMULALLPRDDB01";
#my $SQLSRV	= 	"ResearchDB";
my $SQLDB	= 	"Global_Vantage";
#my $SQLDB	= 	"Global_Distribute";
#my $LIMIT	=	"205577; #v205
#my $LIMIT	=	"206448"; #v206
#my $LIMIT	=	"207503"; #v209
#my $LIMIT	=	"207800"; #v210
#my $LIMIT	=	"209668"; #v216
#my $LIMIT	=	"210605"; #v218
#my $LIMIT	=	"211355"; #v219
my $LIMIT	=	"213368"; #v224

my $HOME	=	"c:\\temp\\ndc_denorm";

# Build distinct ndc_code array
# Connect to the database

	my $dbh;
	my $DSN;
	#$DSN = 'driver={SQL Server};Server=MULQADB02; database=SDK_Distribute;TrustedConnection=Yes'; 
	#$DSN = 'driver={SQL Server};Server=\$SQLSRV; database=\$SQLDB;TrustedConnection=Yes'; 
	
	$DSN = 'driver={SQL Server};Server=IPMULALLPRDDB01; database=Global_Vantage;TrustedConnection=Yes'; 
	#$DSN = 'driver={SQL Server};Server=IPMULALLQADB01; database=Global_Vantage;TrustedConnection=Yes';
	$dbh = DBI->connect("dbi:ODBC:$DSN") or die "$DBI::errstr\n";
	$dbh->{'LongTruncOk'} = 1;
	$dbh->{'LongReadLen'} = 65535;

# Build the ndc_code array

	my $sth = $dbh->prepare("SELECT distinct top $LIMIT ndc_code FROM $table order by ndc_code");
	$sth->execute;	
	
	my @ndc_code_distinct;
	
	while( my $row = $sth->fetchrow_array())
	{
			push @ndc_code_distinct, $row;
	}


my $ndc_code_size = @ndc_code_distinct;

print "ndc_code_size:  $ndc_code_size\n";

#Sqlcmd: Error: Error occurred while opening or operating on file 
#c:\temp\ndc_denorm\sqlsrv_active_ingredient.txt""C:\Program Files\Microsoft SQL Server\110\Tools\Binn\sqlcmd (Reason: The filename, directory name, or volume label syntax is incorrect).

my @ColArray = qw(ndc_code brand_description main_multum_drug_code route_description dose_form_description product_strength_description 
drug_id active_ingredient strength_num_amount strength_num strength_denom_amount strength_denom csa_schedule j_code j_code_description 
otc_status inner_package_size inner_package_description outer_package_size obsolete_date source_desc address1 address2 city state 
province zip country orange_book_description unit_dose_code repackaged gbo awp ful mdr wsc);

#my @ColArray = qw(ndc_code brand_description main_multum_drug_code route_description dose_form_description product_strength_description 
#drug_id active_ingredient strength_num_amount strength_num strength_denom_amount strength_denom csa_schedule j_code j_code_description 
#otc_status inner_package_size inner_package_description outer_package_size obsolete_date source_desc address1 address2 city state 
#province zip country orange_book_description unit_dose_code repackaged);


#my @ColArray = qw(active_ingredient);
#my @ColArray = qw(ndc_code);
#my @ColArray = qw(strength_num_amount);
#my @ColArray = qw(j_code);
#my @ColArray = qw(address1 address2);
#my @ColArray = qw(outer_package_size);
#my @ColArray = qw(province);
#my @ColArray = qw(obsolete_date);
#my @ColArray = qw(obsolete_date outer_package_size ful mdr awp wsc address1 source_desc);
#my @ColArray = qw(address1 source_desc);

	for my $COL (@ColArray)
	{

	
			# Open the file based on the column name and write to disk
			open FILE, ">", "c:\\temp\\ndc_denorm\\sqlsrv$COL.bat" or die $!;
		
			print FILE "\"C:\\Program Files\\Microsoft SQL Server\\110\\Tools\\Binn\\sqlcmd\"";	
			
			if ($COL =~ m/ndc_code/)
			{
				print FILE " -S $SQLSRV -d $SQLDB -E -W -h -1 -s \"|\" -Q \"set nocount on; select top $LIMIT $COL from $table order by ndc_code \" -o \"$HOME\\sqlsrv_$COL.txt\"";
			}
			
			elsif (($COL =~ m/outer_package_size/) || ($COL =~ m/obsolete_date/) || ($COL =~ m/ful/) || ($COL =~ m/mdr/) || ($COL =~ m/awp/) || ($COL =~ m/wsc/))
			{
				print FILE " -S $SQLSRV -d $SQLDB -E -W -h -1 -s \"|\" -Q \"set nocount on; select top $LIMIT $COL from $table order by ndc_code, $COL \" -o \"$HOME\\sqlsrv_$COL.txt\"";
			}
			
			else
			{
				print FILE " -S $SQLSRV -d $SQLDB -E -W -h -1 -s \"|\" -Q \"set nocount on; select top $LIMIT ISNULL($COL, 'NULL') from $table order by ndc_code, $COL \" -o \"$HOME\\sqlsrv_$COL.txt\"";
			}
			close(FILE);
			system ("$HOME\\sqlsrv$COL.bat");


	}

	for my $COL (@ColArray)
	{

#	
		# Open the file based on the column name and write to disk
			open FILE, ">", "c:\\temp\\ndc_denorm\\sqlite$COL.bat" or die $!;
			if ($COL =~ m/ndc_code/)
			{
				print FILE "sqlite3.exe en-US_VantageRx.odb \"select $COL from $table order by ndc_code LIMIT $LIMIT \" > $HOME\\sqlite_$COL.txt";
			}
			
			#elsif ($COL =~ m/mdr/)
			#{
			#	print FILE "sqlite3.exe en-US_VantageRx.odb \"select $COL from $table order by ndc_code, $COL LIMIT $LIMIT \" > $HOME\\sqlite_$COL.txt";
			#}
			
			else
			{
				print FILE "sqlite3.exe en-US_VantageRx.odb \"select  IFNULL($COL, 'NULL') from $table order by ndc_code, $COL LIMIT $LIMIT \" > $HOME\\sqlite_$COL.txt";
			}
		
			close(FILE);
			system ("$HOME\\sqlite$COL.bat");
#		}
#		
	}

use ReadFile;
use File::Copy;


for my $COL (@ColArray)
{
	my @FileArray 		= ReadFile->new("$HOME\\sqlsrv_$COL.txt")->GetFile();
	
	if ($COL =~ m/active_ingredient/)
	{
		my @SortFileArray 	= sort @FileArray;
		@FileArray			= @SortFileArray;
	}
	
# SQLite

	my @SQLiteFileArray 	= ReadFile->new("$HOME\\sqlite_$COL.txt")->GetFile();

	if ($COL =~ m/active_ingredient/)
	{
		my @SortSQLiteFileArray = sort @SQLiteFileArray;
		@SQLiteFileArray		= @SortSQLiteFileArray;
	}

	print "\n";
	my $SQLSRVSize 			= 	@FileArray;
	my $SQLiteFileSize		=	@SQLiteFileArray;
	
	print "COL:  $COL\n";
	print "SQL Server Array size $SQLSRVSize\n";
	print "SQLite Array size $SQLiteFileSize\n";
	
for (my $i = 0; $i < $SQLiteFileSize; $i++)
{

	$FileArray[$i] 			=~ s/\(//g; # remove "("
	$FileArray[$i] 			=~ s/\)//g; # remove ")"
	$SQLiteFileArray[$i]	=~ s/\(//g; # remove "("
	$SQLiteFileArray[$i]	=~ s/\)//g; # remove ")"
	
	$FileArray[$i] 			=~ s/\[//g; # remove "["
	$FileArray[$i] 			=~ s/\]//g; # remove "]"
	$SQLiteFileArray[$i]	=~ s/\[//g; # remove "["
	$SQLiteFileArray[$i]	=~ s/\]//g; # remove "]"
	
	$FileArray[$i] 			=~ s/\+//g; # remove "+"
	$SQLiteFileArray[$i]	=~ s/\+//g; # remove "+"
	
	$FileArray[$i] 			=~ s/\*//g; # remove "*"
	$SQLiteFileArray[$i]	=~ s/\*//g; # remove "*"
	
	$FileArray[$i] 			=~ s/\?//g; # remove "?"
	$SQLiteFileArray[$i]	=~ s/\?//g; # remove "?"
	
	$FileArray[$i] 			=~ s/\'//g; # remove "'"
	$SQLiteFileArray[$i]	=~ s/\'//g; # remove "'"
	

	
	#$FileArray[$i] 			=~ s/(\.\d+?)0+\b/$1/g;		#remove all of the trailing zeros not just at the end of the line
	#$SQLiteFileArray[$i]	=~ s/^0+//;
	
	#$FileArray[$i] 			=~ s/\.0[^\d]//g;			#remove .0 but not if there are decimals to the right of the 0
	#$FileArray[$i] 			=~ s/.000/0/;
	
	if ($COL =~ m/ful/)
	{
			$FileArray[$i]	=~ s/\.000//g;
			$SQLiteFileArray[$i] =~ s/\''/NULL/g;
			$SQLiteFileArray[$i] =~ s/\s+$//g;
			$SQLiteFileArray[$i] =~ s/^0+//; # remove the leading zero
			$SQLiteFileArray[$i] =~ s/^\n/NULL/;
			
			# This reads the string into an array and checks to see if the very first element is a NULL and then literally replaces it
			my $TempFile = $FileArray[$i];
			my @TempArray = split(/ /, $TempFile);
			
			if (@TempArray[0] =~ m/^$/)
			{
				$FileArray[$i] = "NULL";
			}
			
			# This reads the string into an array and checks to see if the very first element is a NULL and then literally replaces it
			my $SQLiteTempFile = $SQLiteFileArray[$i];
			my @SQLiteTempArray = split(/ /, $SQLiteTempFile);
			
			if (@SQLiteTempArray[0] =~ m/^$/)
			{
				$SQLiteFileArray[$i] = "NULL";
			}
					
	}
	
	if ($COL =~ m/wsc/)
	{
			#$FileArray[$i]	=~ s/NULL/\000/;
			#print "chr($FileArray[$i]\n";
			$FileArray[$i]	=~ s/\.000/NULL/g;
			$SQLiteFileArray[$i] =~ s/^0+//; # remove the leading zero
			#chomp($FileArray[$i]);
			#chomp($SQLiteFileArray[$i]);
			
			# This reads the string into an array and checks to see if the very first element is a NULL and then literally replaces it
			my $SQLiteTempFile = $SQLiteFileArray[$i];
			my @SQLiteTempArray = split(/ /, $SQLiteTempFile);
			
			if (@SQLiteTempArray[0] =~ m/^$/)
			{
				$SQLiteFileArray[$i] = "NULL";
			}
	}
	
		if ($COL =~ m/city/)
	{

		$FileArray[$i] 			=~ s/\,//g; # remove ","
		$SQLiteFileArray[$i]	=~ s/\,//g; # remove ","
	}
	
	if ($COL =~ m/j_code/)
	{
			#$FileArray[$i]	=~ s/NULL/\0/g;
			#$SQLiteFileArray[$i] =~ s/''/NULL/g;
			$FileArray[$i] =~ s/\s+$/NULL/g; # remove the extra spaces at the end

	}
	
	if ($COL =~ m/province/)
	{
			
			$FileArray[$i] =~ s/\s+$/NULL/g; # remove the extra spaces at the end
			$FileArray[$i] =~ s/\n/NULL/g;

			my $TempFile = $FileArray[$i];
			my @TempArray = split(/ /, $TempFile);
			
			if (!defined(@TempArray[0]) && (@TempArray[0] =~ m/^$/))
			#if (@TempArray[0] =~ m/^$/)
			{
				$FileArray[$i] = "NULL";
			}
			
	}
	
	if ($COL =~ m/source_desc/)
	{
			
			$FileArray[$i]	=~ s/\’//g; # Substitution of "ä"
			$SQLiteFileArray[$i] =~ s/\’//g; # Substitution of "ä"
			$SQLiteFileArray[$i] =~ s/Physician’s/Physicians/g; # Substitution of "ä"
	}
	
	if ($COL =~ m/address1/)
	{
			$FileArray[$i]	=~ s/ä/a/g; # Substitution of "ä"
			$SQLiteFileArray[$i] =~ s/â/a/g; # Substitution of "ä"
			$FileArray[$i]	=~ s/é/e/g; # Substitution of "é"
			$SQLiteFileArray[$i] =~ s/é/e/g; # Substitution of "é"
			$FileArray[$i] =~ s/\s+$/NULL/g; # remove the extra spaces at the end
			$FileArray[$i] =~ s/\n/NULL/g;
			#ää
			# This reads the string into an array and checks to see if the very first element is a NULL and then literally replaces it
			my $TempFile = $FileArray[$i];
			my @TempArray = split(/ /, $TempFile);
			
			if (@TempArray[0] =~ m/^$/)
			{
				$FileArray[$i] = "NULL";
			}
			
			# This reads the string into an array and checks to see if the very first element is a NULL and then literally replaces it
			my $SQLiteTempFile = $SQLiteFileArray[$i];
			my @SQLiteTempArray = split(/ /, $SQLiteTempFile);
			
			if (@SQLiteTempArray[0] =~ m/^$/)
			{
				$SQLiteFileArray[$i] = "NULL";
			}
			
	}
	
		if ($COL =~ m/address2/)
	{
			#$FileArray[$i]	=~ s/^[^\f]{1}\z/NULL/g;
			#$FileArray[$i]	=~ s/^[\n]$/NULL/g; #keep this one for 88098 or not
			#$SQLiteFileArray[$i] =~ s/NULL//g;
			#$FileArray[$i] =~ s/\s+$/NULL/g; # remove the extra spaces at the end
			#$FileArray[$i] =~ s/\n/NULL/g;
			#$FileArray[$i] =~ s/[\c13]/NULL/g;
			#$FileArray[$i] =~ s/\s\r$/"NULL"/g;
			#$FileArray[$i] =~ s/[' ']/NULL/;
			
			my $TempFile = $FileArray[$i];
			my @TempArray = split(/ /, $TempFile);
			
			if (@TempArray[0] =~ m/^$/)
			{
				$FileArray[$i] = "NULL";
			}
			
			
			#for my $tFile (@TempArray)
			#{
			#	print "SplitArray:  $tFile\n";
			#}
			
			#my $TempCount = @TempArray;
			#print "TempCountSize:  $TempCount\n";
			

			
			#print "$i: $FileArray[$i]\n";
			#$FileArray[$i] = "NULL";
			#if ($FileArray[$i] =~ m/^$/) 
			#if ($FileArray[$i] =~ m/^$/) 
			#{
			#	$FileArray[$i] = "NULL";
			#	print "CHANGED $i: $FileArray[$i]\n";
			#}

	}
	
	if ($COL =~ m/strength_num_amount/)
	{
			if ($SQLiteFileArray[$i] != m/0/)
			{
				$SQLiteFileArray[$i] =~ s/^0+//; # remove the leading zero
			}
	}

	if ($COL =~ m/strength_denom_amount/)
	{
			#if (defined($SQLiteFileArray[$i]) && ($SQLiteFileArray[$i] !~ m/0/))
			if ($SQLiteFileArray[$i] != m/0/)
			{
				$SQLiteFileArray[$i] =~ s/^0+//; # remove the leading zero
			}
	}

	if ($COL =~ m/inner_package_size/)
	{
			#if (defined($SQLiteFileArray[$i]) && ($SQLiteFileArray[$i] !~ m/0/))
			if ($SQLiteFileArray[$i] != m/"0"/)
			{
				#print "I:  $i\n";
				$SQLiteFileArray[$i] =~ s/^0+//; # remove the leading zero
			}
	}

	if ($COL =~ m/awp/)
	{
			#if (defined($SQLiteFileArray[$i]) && ($SQLiteFileArray[$i] !~ m/0/))
			if ($SQLiteFileArray[$i] != m/0/)
			{
				#print "$SQLiteFileArray[$i]\n";
				$SQLiteFileArray[$i] =~ s/^0+//; # remove the leading zero
			}
	}
	
	if ($COL =~ m/mdr/)
	{
		#print "$SQLiteFileArray[$i]\n";			
			if ($SQLiteFileArray[$i] != m/0/)
			#if (defined($SQLiteFileArray[$i]) && ($SQLiteFileArray[$i] !~ m/0/))
			#if ($SQLiteFileArray[$i] ne m/0/)			
			{
			
				$SQLiteFileArray[$i] =~ s/^0+//; # remove the leading zero
			}
	}
	
	if ($FileArray[$i] =~ m/$SQLiteFileArray[$i]/)
	{
		#print "$FileArray[$i] MATCHS $SQLiteFileArray[$i] at INDEX $i at $COL\n";
	}
	else 
	{
		print "$FileArray[$i] NOMATCH $SQLiteFileArray[$i] at INDEX $i at $COL\n";
	}
	
}


}
