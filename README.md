# Nestbox

A PHP Data Objects (PDO) wrapper class for databases manipulations to easily fill gaps of niche requirements. This
project was designed with simplistic usage with database safety in mind. It is updated as needs arise and is probably a
result of NIH syndrome.

## Installing

*todo: add how to install here*

## Basic Usage

```php
use NestboxPHP\Nestbox;

$nest = new Nestbox($dbHost, $dbUser, $dbPass, $dbName);

try {
    if( $nest->query_execute( "SELECT * FROM `users`;" )) {
        $users = $nest->results();
    }
} catch ( NestboxException $exception ) {
    die( $exception->getMessage());
}
```

# References
Since this was a project designed for learning, here are some great references used during the creation of this project:
- [(The only proper) PDO tutorial](https://phpdelusions.net/pdo)
- [Roll your own PDO PHP Class](http://culttt.com/2012/10/01/roll-your-own-pdo-php-class/)
- [Why You Should Be Using PHP's PDO for Database Access](http://code.tutsplus.com/tutorials/why-you-should-be-using-phps-pdo-for-database-access--net-12059)
