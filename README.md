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
## Packages

Nestbox is meant to be a base PDO wrapper upon which to build. The following packages (Birds) are currenly in development or supported:

| Package | Description |
|---|---|
| [Babbler](https://github.com/NestboxPHP/Babbler) | Content management for website/blog functionality. |
| [Bullfinch](https://github.com/NestboxPHP/Bullfinch) | Message board management. |
| [Cuckoo](https://github.com/NestboxPHP/Cuckoo) | Transparent in-line encryption for queries. |
| [Lorikeet](https://github.com/NestboxPHP/Lorikeet) | Image upload processing and indexing. |
| [Macaw](https://github.com/NestboxPHP/Macaw) | An interface for the Microsoft PlayFab REST API. |
| [Magpie](https://github.com/NestboxPHP/Magpie) | User and role permissions manager. |
| [Sternidae](https://github.com/NestboxPHP/Sternidae) | Historical and future flight tracking tool. |
| [Titmouse](https://github.com/NestboxPHP/Titmouse) | User registration and session management with built-in password best-practices. |
| [Veery](https://github.com/NestboxPHP/Veery) | Weather forecast data collection, storage, and analysis. |
| [Weaver](https://github.com/NestboxPHP/Weaver) | REST API endpoint management. |

# References
Since this was a project that originated with the intent of learning how to do PDO things, here are some great references used during the development of this project:
- [(The only proper) PDO tutorial](https://phpdelusions.net/pdo)
- [Roll your own PDO PHP Class](http://culttt.com/2012/10/01/roll-your-own-pdo-php-class/)
- [Why You Should Be Using PHP's PDO for Database Access](http://code.tutsplus.com/tutorials/why-you-should-be-using-phps-pdo-for-database-access--net-12059)
