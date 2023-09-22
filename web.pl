#!/usr/bin/perl
{
package WebServer;
use lib './';
use strict;
use DBI;
use base qw(HTTP::Server::Simple::CGI);


sub handle_request {
    my $self = shift;
    my $cgi  = shift;
   
    print "HTTP/1.0 200 OK\r\n";
    my $email = $cgi->param('email');
    
    print "Content-Type: text/html\n\n";
    print "<html><body>";
    print '<meta charset="UTF-8">';
    print "<div style=\"width:80%;margin:0 auto\">";
    print "<h1>Введите email для поиска</h1>";
    print $cgi->start_form("GET", "/", "");
    print $cgi->textfield('email', $email, 150, 80);
    print $cgi->submit('search', 'Искать');
    print $cgi->end_form;        

    if ( $email ) {
        my $host     = 'localhost';
        my $dbase    = 'maillog';
        my $user     = 'logger';
        my $password = 'pa77wor1';
        my $dbh = DBI->connect("DBI:mysql:database=$dbase;host=$host", $user, $password,
                            { RaiseError => 1, mysql_server_prepare => 1 });
        
        my $sth = $dbh->prepare("
            WITH result AS (
                SELECT l.address address, l.created l_created, l.int_id l_int_id, l.str l_str, m.created m_created, m.int_id m_int_id, m.str m_str
                , FIRST_VALUE( IF(m.created, m.created, l.created) ) OVER ( PARTITION BY l.int_id ORDER BY l.created ) AS ftime
                    FROM log l 
                    LEFT JOIN message m ON l.int_id = m.int_id
                    WHERE l.address = ?
                    ORDER BY ftime, l.int_id, l.created
            )
            SELECT l_created as created, l_str as str, l_int_id as int_id, ftime FROM result
                UNION
            SELECT m_created as created, m_str as str, m_int_id as int_id, ftime FROM result WHERE m_created IS NOT NULL 
            ORDER BY ftime, int_id, created
            LIMIT 101
        ");
        $sth->execute($email);

        print "\n<h2>Результаты</h2>";
        print "<table>";
        my $number = 0;
        while ( my @row = $sth->fetchrow_array ) {
            $number++;
            if ( $number < 101 ) {
                print "<tr><td>$number</td><td nowrap>$row[0]</td><td>$row[1]</td></tr>";
            } else {
                print "<tr><td colspan=\"3\">Найдено больше 100 строк</td></tr>";
            }
        }
        print "</table>";
    }
    print "</div></body></html>";
         
}
 

} 
 
# start the server on port 3000
my $server = WebServer->new(3000);
$server->run();


