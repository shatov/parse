#!/usr/bin/perl
use v5.32;
use strict;
use DBI;
use Time::HiRes qw(gettimeofday tv_interval);
#use Time::Local;
#use Data::Dumper;

#my @driver_names = DBI->available_drivers;
#my %drivers      = DBI->installed_drivers;
##my @data_sources = DBI->data_sources($driver_name, \%attr);
#
#print Dumper(@driver_names);
#print Dumper(%drivers);
#die;
my $t0 = [gettimeofday];
my $filename = 'biglog.txt';

# Connect to the database.
my $host     = 'localhost';
my $dbase    = 'maillog';
my $user     = 'logger';
my $password = 'pa77wor1';
my $dbh      = DBI->connect("DBI:mysql:database=$dbase;host=$host", $user, $password,
                    { RaiseError => 1, mysql_server_prepare => 1 });

my $sth_mes = $dbh->prepare("INSERT INTO message (created, id, int_id, str) VALUES (?, ?, ?, ?)");
my $sth_log = $dbh->prepare("INSERT INTO log (created, int_id, str, address) VALUES (?, ?, ?, ?)");

open ( my $fh, "<", $filename ) or die "Can't open < $filename: $!";

#open(my $fh2, '>', 'report.csv');


#2012-02-13 14:59:15 1Rwtcp-0000Ac-BW <= tpxmuwr@somehost.ru H=mail.somehost.com [84.154.134.45] P=esmtp S=1486 id=120213145115.FAXCHADMNIC.888573@whois.somehost.ru
#2012-02-13 14:59:15 1Rwtcp-0000Ac-BW == pmtzjnyta@brightconsult.ru R=dnslookup T=remote_smtp defer (-1): domain matches queue_smtp_domains, or -odqs set
#2012-02-13 15:02:15 1Rwtcp-0000Ac-BW Spool file is locked (another process is handling this message)
#2012-02-13 15:02:27 1Rwtcp-0000Ac-BW ** pmtzjnyta@brightconsult.ru R=dnslookup T=remote_smtp: SMTP error from remote mail server after RCPT TO:<pmtzjnyta@brightconsult.ru>: host tmg.navicons.ru [86.110.12.210]: 550 5.7.1 :127.0.0.5:Client host 194.226.74.6 blocked using 88.blocklist.zap; Mail from IP banned. To request removal from this list please forward this message to dfrvhdkjrrrkkqjpv@messaging.microsoft.com
#2012-02-13 15:02:28 1Rwtfv-0002ry-W4 <= <> R=1Rwtcp-0000Ac-BW U=mailnull P=local S=2984
#2012-02-13 15:02:28 1Rwtfv-0002ry-W4 => :blackhole: <tpxmuwr@somehost.ru> R=blackhole_router
#2012-02-13 15:02:28 1Rwtfv-0002ry-W4 Completed
#2012-02-13 15:02:51 SMTP connection from [109.70.26.4] (TCP/IP connection count = 1)
my $count = 0;
while (my $line = <$fh>) {
    chomp $line;
    my ( $data, $time, $str, $int_id, $flag, $other) = $line =~ /^
        (.*?)\s       # $1 Год Месяц Дата
        (.*?)\s       # $4 Часы Минуты Секунды
        (                               # $3 str без временной метки
            (.{6}-.{6}-.{2} | )         # $4 int_id или ничего
            \s?                     
            ( <= | => | -> | \*\* | == |  )  # $5 флаг или его отсутствие
            \s?
            (.*$)                       # $6 остальное
        )/x;
    
    #my $created = timegm( $sec, $min, $hour, $day, $month-1, $year-1900 );
    my $created = "$data $time";
    if ( $flag eq '<=' ) {
        my ($id_mes) = $str =~ /id=(.*?)(\s|$)/;
        
        if ( $id_mes ) {
            # вставляем в бд message
            # created, id, int_id, str
            $sth_mes->execute( $created, $id_mes, $int_id, $str );
            #say $id_mes;    
        }
        else {
            # вставляем в log    
            # created, int_id, str, address
            $sth_log->execute( $created, $int_id, $str, undef );
        }
        $count++;
    }
    else {
        my ($address) = $str =~ /(?: <= | => | -> | \*\* | ==) \s (\S+? \@ \S+? \. \S+?) (?: \s | :)/x;        
        #say "str $created $int_id $str";
        #say $address;
        $sth_log->execute( $created, $int_id, $str, $address );
    }
    #say "prm $5, $6, $7";
    #say "mes $8";
    #say;
    #last if $count == 4;
    
    
    #say $fh2 "$1;$3;$8;$9;$10";
    #
    # ... do something interesting with $line here ...
    #
}
say $count if !($a % 1000);

close $fh;
#close $fh2;

$dbh->disconnect();
say tv_interval ( $t0, [gettimeofday]);

1;

