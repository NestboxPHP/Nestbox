<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox;

use Exception;
use PDO;
use PDOException;
use PDOStatement;
use Random\RandomException;
use NestboxPHP\Nestbox\Exception\DuplicateTableException;
use NestboxPHP\Nestbox\Exception\InvalidParameterValueType;
use NestboxPHP\Nestbox\Exception\EmptyQueryException;
use NestboxPHP\Nestbox\Exception\FailedToBindValueException;
use NestboxPHP\Nestbox\Exception\InvalidColumnException;
use NestboxPHP\Nestbox\Exception\InvalidSchemaSyntaxException;
use NestboxPHP\Nestbox\Exception\InvalidTableException;
use NestboxPHP\Nestbox\Exception\EmptyParamsException;
use NestboxPHP\Nestbox\Exception\InvalidWhereOperator;
use NestboxPHP\Nestbox\Exception\MalformedJsonException;
use NestboxPHP\Nestbox\Exception\MismatchedColumnNamesException;
use NestboxPHP\Nestbox\Exception\MissingDatabaseHostException;
use NestboxPHP\Nestbox\Exception\MissingDatabaseNameException;
use NestboxPHP\Nestbox\Exception\MissingDatabasePassException;
use NestboxPHP\Nestbox\Exception\MissingDatabaseUserException;
use NestboxPHP\Nestbox\Exception\MissingParametersException;
use NestboxPHP\Nestbox\Exception\NestboxException;
use NestboxPHP\Nestbox\Exception\QueryErrorException;
use NestboxPHP\Nestbox\Exception\TransactionBeginFailedException;
use NestboxPHP\Nestbox\Exception\TransactionCommitFailedException;
use NestboxPHP\Nestbox\Exception\TransactionException;
use NestboxPHP\Nestbox\Exception\TransactionInProgressException;
use NestboxPHP\Nestbox\Exception\TransactionRollbackFailedException;

class Nestbox
{
    protected const PACKAGE_NAME = 'nestbox';

    // connection properties
    protected string $host = 'localhost';
    protected string $user = 'root';
    protected string $pass = '';
    protected string $name = '';

    protected PDO $pdo;
    protected PDOStatement $stmt;

    protected array $tableSchema = [];
    protected array $triggerSchema = [];

    public const nestbox_settings_table = 'nestbox_settings';

    use MiscellaneousFunctionsTrait;


    /**
     * Default constructor
     *
     * @param string|null $host
     * @param string|null $user
     * @param string|null $pass
     * @param string|null $name
     * @throws MissingDatabaseHostException
     * @throws MissingDatabaseUserException
     * @throws MissingDatabasePassException
     * @throws MissingDatabaseNameException
     */
    public function __construct(string $host = null, string $user = null, string $pass = null, string $name = null)
    {
        // start session if unstarted
        if (PHP_SESSION_ACTIVE !== session_status()) session_start();

        // define new constants for future calls
        if ($host && !defined('NESTBOX_DB_HOST')) define('NESTBOX_DB_HOST', $host);
        if ($user && !defined('NESTBOX_DB_USER')) define('NESTBOX_DB_USER', $user);
        if ($pass && !defined('NESTBOX_DB_PASS')) define('NESTBOX_DB_PASS', $pass);
        if ($name && !defined('NESTBOX_DB_NAME')) define('NESTBOX_DB_NAME', $name);

        // null and undefined values mean missing data
        if (is_null($host) && !defined('NESTBOX_DB_HOST')) throw new MissingDatabaseHostException();
        if (is_null($user) && !defined('NESTBOX_DB_USER')) throw new MissingDatabaseUserException();
        if (is_null($pass) && !defined('NESTBOX_DB_PASS')) throw new MissingDatabasePassException();
        if (is_null($name) && !defined('NESTBOX_DB_NAME')) throw new MissingDatabaseNameException();

        // manual overrides take precedence for new or invoked instantiations, otherwise use constants
        $this->host = ($host) ?: NESTBOX_DB_HOST;
        $this->user = ($user) ?: NESTBOX_DB_USER;
        $this->pass = ($pass) ?: NESTBOX_DB_PASS;
        $this->name = ($name) ?: NESTBOX_DB_NAME;


        // make sure class tables have been created
        $this->check_class_tables();

        // load settings
        $this->load_settings();
    }


    /**
     * Magic method to reset pdo connection details
     *
     * @param string|null $host
     * @param string|null $user
     * @param string|null $pass
     * @param string|null $name
     * @return void
     */
    public function __invoke(string $host = null, string $user = null, string $pass = null, string $name = null): void
    {
        // save settings
        $this->save_settings();

        // close any existing database connection
        $this->close();

        // reconnect to defined database
        $this->__construct($host, $user, $pass, $name);
    }


    /**
     * Default destructor
     */
    public function __destruct()
    {
        // save settings
        $this->save_settings();

        // close any existing database connection
        $this->close();
    }


    /**
     * Class Tables
     *   ____ _                 _____     _     _
     *  / ___| | __ _ ___ ___  |_   _|_ _| |__ | | ___  ___
     * | |   | |/ _` / __/ __|   | |/ _` | '_ \| |/ _ \/ __|
     * | |___| | (_| \__ \__ \   | | (_| | |_) | |  __/\__ \
     *  \____|_|\__,_|___/___/   |_|\__,_|_.__/|_|\___||___/
     *
     */


    /**
     * Checks that all class tables have been created, and if not, create them
     *
     * @return void
     */
    protected function check_class_tables(): void
    {
        foreach (get_class_methods($this) as $methodName) {
            if (preg_match('/^create_class_table_(\w+)$/', $methodName, $matches)) {
                if (!$this->valid_schema($matches[1])) $this->create_class_tables();
            }
        }
    }


    /**
     * Calls all class methods that start with `create_class_table_`
     *
     * @return void
     */
    protected function create_class_tables(): void
    {
        foreach (get_class_methods($this) as $methodName) {
            if (str_starts_with(haystack: $methodName, needle: "create_class_table_")) $this->$methodName();
        }
        $this->load_table_schema(forceReload: true);
    }


    /**
     * Connections
     *   ____                            _   _
     *  / ___|___  _ __  _ __   ___  ___| |_(_) ___  _ __  ___
     * | |   / _ \| '_ \| '_ \ / _ \/ __| __| |/ _ \| '_ \/ __|
     * | |__| (_) | | | | | | |  __/ (__| |_| | (_) | | | \__ \
     *  \____\___/|_| |_|_| |_|\___|\___|\__|_|\___/|_| |_|___/
     *
     */


    /**
     * Connect to the database, returns `true` on a successful connect, otherwise `false`
     *
     * @return bool
     * @throws NestboxException
     */
    public function connect(): bool
    {
        // check for existing connection
        if ($this->check_connection()) return true;

        // MySQL Database
        try {
            $this->pdo = new PDO(
                "mysql:host=$this->host;dbname=$this->name",
                $this->user,
                $this->pass,
                [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                    PDO::ATTR_PERSISTENT => true,
                    PDO::ATTR_EMULATE_PREPARES => true, // off for :named placeholders
                    PDO::ATTR_STRINGIFY_FETCHES => false,
                ]
            );
        } catch (PDOException $e) {
            throw new NestboxException($e->getMessage());
        }

        // successful connection
        return true;
    }


    /**
     * Check if a connection exists, returns `true` if one exists, otherwise `false`
     *
     * @return bool
     */
    public function check_connection(): bool
    {
        if (!empty($this->pdo)) {
            // test existing connection for timeout
            $this->prep("SELECT 1");
            $this->execute();
            $rows = $this->fetch_all_results();

            // check test results
            if (1 === $rows[0]['1']) return true;

            // kill dead connection
            $this->close();
        }

        return false;
    }


    /**
     * Close an existing connection
     *
     * @return void
     */
    public function close(): void
    {
        // "To close the connection, you need to destroy the object"
        // - https://www.php.net/manual/en/pdo.connections.php
        unset($this->pdo);
    }


    /**
     * Query Execution
     *   ___                          _____                     _   _
     *  / _ \ _   _  ___ _ __ _   _  | ____|_  _____  ___ _   _| |_(_) ___  _ __
     * | | | | | | |/ _ \ '__| | | | |  _| \ \/ / _ \/ __| | | | __| |/ _ \| '_ \
     * | |_| | |_| |  __/ |  | |_| | | |___ >  <  __/ (__| |_| | |_| | (_) | | | |
     *  \__\_\\__,_|\___|_|   \__, | |_____/_/\_\___|\___|\__,_|\__|_|\___/|_| |_|
     *                        |___/
     */


    /**
     * Executes a query against the database, returns `true` on success, otherwise `false`
     *
     * @param string $query Non-empty string with SQL query
     * @param array $params Array of key=>value pairs for :named paramaters
     * @param bool $close Close the connection upon query execution
     * @return bool
     * @throws EmptyQueryException
     */
    public function query_execute(string $query, array $params = [], bool $close = false): bool
    {
        // check query emptiness
        if (empty(trim($query))) throw new EmptyQueryException;

        // connect to database
        $this->connect();

        // prepare statement and bind parameters
        $params = $this::validate_parameters($query, $params);
        $this->prep($query, $params);
        if ($params) foreach ($params as $col => $val) $this->bind($col, $val);

        // execute, close if applicable, and return resutlts
        $result = $this->execute();
        if ($close) $this->close();
        return $result;
    }


    /**
     * Prepare a query statement, returns `true` on success, otherwise `false`
     *
     * @param string $query
     * @param array|$params
     * @return bool
     */
    protected function prep(string $query, array $params = []): bool
    {
        // prepare a statement without parameters
        if (empty($params)) {
            if ($this->stmt = $this->pdo->prepare($query)) return true;
            return false;
        }

        // prepare a statement with parameters
        if ($this->stmt = $this->pdo->prepare($query, $params)) return true;
        return false;
    }


    /**
     * Bind a variable to a named parameter, returns `true` on success, otherwise `false`
     * TODO: update Exceptions for message creation
     *
     * @param $variable
     * @param $value
     * @return bool
     * @throws InvalidParameterValueType
     * @throws FailedToBindValueException
     */
    protected function bind($variable, $value): bool
    {
        // set binding type
        $type = Nestbox::get_parameter_pdo_type($value);
        if (!$type) throw new InvalidParameterValueType("$variable => ($type) $value");

        // backwards compatibility or whatever
        $variable = (!str_starts_with($variable, ':')) ? ":$variable" : $variable;

        // bind value to parameter
        if (true === $this->stmt->bindValue($variable, $value, $type)) return true;

        // we didn't do it
        throw new FailedToBindValueException("$variable => ($type) $value");
    }


    /**
     * Checks variable type for parameter binding
     *
     * @param $parameter
     * @return int|bool
     */
    public static function get_parameter_pdo_type($parameter): int|bool
    {
        if (is_int($parameter)) return PDO::PARAM_INT;
        if (is_bool($parameter)) return PDO::PARAM_BOOL;
        if (is_null($parameter)) return PDO::PARAM_NULL;
        if (is_array($parameter)) return false;
        return PDO::PARAM_STR;
    }


    /**
     * Execute a statement or throws either a QueryErrorException or a PDOException
     *
     * @return bool
     * @throws QueryErrorException
     * @throws PDOException
     */
    protected function execute(): bool
    {
        // execute query
        try {
            if ($this->stmt->execute()) return true;
            throw new QueryErrorException(errorInfo: $this->stmt->errorInfo());
        } catch (PDOException $e) {
            throw new PDOException("PDO Exception: {$e->getMessage()}");
        }
    }


    /**
     * Generic PDO Helpers
     *   ____                      _        ____  ____   ___    _   _      _
     *  / ___| ___ _ __   ___ _ __(_) ___  |  _ \|  _ \ / _ \  | | | | ___| |_ __   ___ _ __ ___
     * | |  _ / _ \ '_ \ / _ \ '__| |/ __| | |_) | | | | | | | | |_| |/ _ \ | '_ \ / _ \ '__/ __|
     * | |_| |  __/ | | |  __/ |  | | (__  |  __/| |_| | |_| | |  _  |  __/ | |_) |  __/ |  \__ \
     *  \____|\___|_| |_|\___|_|  |_|\___| |_|   |____/ \___/  |_| |_|\___|_| .__/ \___|_|  |___/
     *                                                                      |_|
     */


    /**
     * Takes an array of parameters and compares it against a query and returns a new array with only named parameters
     * within the query and their associated value, or throws an exception if parameters from the query are missing
     *
     * @param string $query
     * @param array $params
     * @return array
     * @throws MissingParametersException
     */
    public static function validate_parameters(string $query, array $params): array
    {
        $output = [];
        preg_match_all('/:(\w+)/', $query, $queryParams);
        $queryParams = $queryParams[1];

        foreach ($params as $key => $value) {
            // verify parameter is in query
            if (!strpos($query, ":" . trim($key, ":"))) continue;

            // verify parameter is a valid schema string
            if (!$key = self::valid_schema_string($key)) continue;

            // remove found parameter from missing results
            $position = array_search(trim($key, ":"), $queryParams);
            if (false !== $position) {
                unset($queryParams[$position]);
                $queryParams = array_values($queryParams);
            }

            $output[$key] = $value;
        }

        // oh no, we forgot to pass one or more parameters!
        if ($queryParams) throw new MissingParametersException($query . json_encode($queryParams));

        return $output;
    }


    /**
     * Ensures the conjunction provided for *WHERE* clause is *AND* or *OR*, otherwise returns *AND*
     *
     * @param string $conjunction
     * @return string "AND" | "OR"
     */
    public static function validate_conjunction(string $conjunction): string
    {
        return (preg_match("/^(and|or)$/i", trim($conjunction), $con)) ? $con[0] : "AND";
    }


    /**
     * Generates an array of column names from a given list of parameters by using the keys of the first element in a
     * multi-dimensional array, or the keys of all elements in a flat array
     *
     * @param array $params
     * @return array [col_name_1, col_name_2, ...]
     */
    public static function generate_column_list_from_params(array $params): array
    {
        return (is_array(current($params))) ? array_keys(current($params)) : array_keys($params);
    }


    public function generate_values_clause(string $table, array $values): array
    {
        $clause = [];
        $params = [];
        foreach ($values as $column => $value) {
            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
            $clause[] = ":$column";
            $params[$column] = $value;
        }
        $clause = ($params) ? "VALUES " . implode(", ", $clause) : "";
        return [$clause, $params];
    }


    /**
     * Takes an array of *[`col_name` => `col_val`]* and generates a string of `"$table.$column = new.$column"` values
     * separated by commas to be used in the update clauses of queries
     *
     * @param string $table
     * @param array $updates
     * @return array
     * @throws InvalidColumnException
     */
    public function generate_update_clause_from_params(string $table, array $updates): array
    {
        $clause = [];
        $params = [];
        $primaryKey = $this->table_primary_key($table);

        foreach ($updates as $column => $value) {
            // skip the primary key or invalid table columns
            if ($column == $primaryKey) continue;

            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);

            // we have a value of value with an updated value
            $clause[] = "`$table`.`$column` = `new`.`$column`";
        }

        $clause = " AS `new` ON DUPLICATE KEY UPDATE " . implode(", ", $clause);
        return [$clause, $params];
    }


    /**
     * Generates and returns a string of `col_name = :col_name` values separated by commas, along with an array of
     * [col_name => col_value] to match the set clause string
     *
     * @param string $table
     * @param array $updates
     * @return array [set_string, params]
     */
    public function generate_set_clause(string $table, array $updates): array
    {
        $clause = [];
        $params = [];
        foreach ($updates as $column => $value) {
            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
            $clause[] = "`$column` = :$column";
            $params[$column] = $value;
        }
        $clause = ($params) ? "SET " . implode(", ", $clause) : "";
        return [$clause, $params];
    }


    /**
     * Takes an array of parameters and generates a string of `"(:named_1, :named_2), ..."` named parameters, separated
     * by commas, and their associated values in a newly created parameters array for use in an INSERT statement in the
     * VALUES clause
     *
     * @param array $params
     * @return array ["(:named_1, :named_2), ...", ["named_1" => val_1, "named_2" => val_2, ...]
     */
    public static function generate_named_values_list(array $params): array
    {
        $namedParams = [];
        $namedFields = [];

        if (!is_array(current($params))) $params = [$params];

        foreach ($params as $row => $values) {
            $fields = [];
            foreach ($values as $col => $val) {
                $namedParams["{$col}_$row"] = $val;
                $fields[] = "{$col}_$row";
            }
            $namedFields[] = ":" . implode(", :", $fields);
        }

        return ["( " . implode(" ), ( ", $namedFields) . " )", $namedParams];
    }


    /**
     * Generates a `WHERE col = :col [AND|OR ...]" string for valid `$where` parameters, an empty string if `$where` is
     * empty, or throws an Exception if one or more column names are invalid
     *
     * @param string $table
     * @param array $where keys can include any of the following operators with the column name: "=", ">", "<", ">=",
     *  "<=", "<>", "!=", "BETWEEN", "LIKE", "IN"
     * @param string $conjunction can be "AND" or "OR", case-insensitive
     * @return array
     */
    public function generate_where_clause(string $table, array $where, string $conjunction): array
    {
        $clause = [];
        $params = [];
        foreach ($where as $column => $value) {
            list($column, $operator) = $this::parse_where_operator($column);
            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
            $clause[] = "`$column` $operator :$column";
            $params[$column] = $value;
        }
        $clause = ($params) ? "WHERE " . implode($this::validate_conjunction($conjunction), $clause) : "";
        return [$clause, $params];
    }


    /**
     * Generates a "LIMIT $start, $limit" string if `$start` and `$limit` are greater than *0*, a "LIMIT $limit" if only
     * `$limit` is greater than *0*, otherwise returns an empty string
     *
     * @param int $start
     * @param int $limit
     * @return string
     */
    public static function generate_limit_clause(int $start = 0, int $limit = 0): string
    {
        if ($limit) return ($start) ? "LIMIT $start, $limit" : "LIMIT $limit";
        return "";
    }


    /**
     * Parses the operator portion of a string used for where, such as `col_name =`, where the return array would be
     * *["col_name", "="]*; the column names are **NOT** validated against a schema
     *
     * @param string $columnWithOperator
     * @return array
     * @throws InvalidWhereOperator
     */
    public static function parse_where_operator(string $columnWithOperator): array
    {
        if (!preg_match('/^`?(\w+)`?\s+([<!=>]{1,2}|between|like|in)$/i', trim($columnWithOperator), $matches)) {
            $matches = [$columnWithOperator, self::valid_schema_string($columnWithOperator), "="];
        }

        if (!in_array(strtoupper($matches[2]), ["=", ">", "<", ">=", "<=", "<>", "!=", "BETWEEN", "LIKE", "IN"])) {
            throw new InvalidWhereOperator($matches[2]);
        }

        return [$matches[1], strtoupper($matches[2])];
    }


    /**
     * Query Results
     *   ___                          ____                 _ _
     *  / _ \ _   _  ___ _ __ _   _  |  _ \ ___  ___ _   _| | |_ ___
     * | | | | | | |/ _ \ '__| | | | | |_) / _ \/ __| | | | | __/ __|
     * | |_| | |_| |  __/ |  | |_| | |  _ <  __/\__ \ |_| | | |_\__ \
     *  \__\_\\__,_|\___|_|   \__, | |_| \_\___||___/\__,_|_|\__|___/
     *                        |___/
     */


    /**
     * Return complete result set
     *
     * @return array|false
     */
    public function fetch_all_results(): array|false
    {
        return $this->stmt->fetchAll();
    }


    /**
     * Fetch first row from result set
     *
     * @return array|false
     */
    public function fetch_first_result(): array|false
    {
        return $this->stmt->fetchAll()[0] ?? false;
    }


    /**
     * Fetch next result in a set
     *
     * @return array|false
     */
    public function fetch_next_result(): array|false
    {
        return $this->stmt->fetch();
    }


    /**
     * Return the row count from the most recent query
     *
     * @return int
     */
    public function get_row_count(): int
    {
        return $this->stmt->rowCount();
    }


    /**
     * Get the row ID of the last insert of the active connection
     *
     * @return string|false
     */
    public function get_last_insert_id(): string|false
    {
        return $this->pdo->lastInsertId();
    }


    /**
     * Quick Queries
     *   ___        _      _       ___                  _
     *  / _ \ _   _(_) ___| | __  / _ \ _   _  ___ _ __(_) ___  ___
     * | | | | | | | |/ __| |/ / | | | | | | |/ _ \ '__| |/ _ \/ __|
     * | |_| | |_| | | (__|   <  | |_| | |_| |  __/ |  | |  __/\__ \
     *  \__\_\\__,_|_|\___|_|\_\  \__\_\\__,_|\___|_|  |_|\___||___/
     *
     */


    /**
     * Inserts one or more rows of data, updating existing rows based on primary key
     *
     * @param string $table
     * @param array $rows
     * @param bool $updateOnDuplicate
     * @return int|bool
     * @throws EmptyParamsException
     * @throws MismatchedColumnNamesException
     * @throws InvalidColumnException
     * @throws InvalidTableException
     */
    public function insert(string $table, array $rows, bool $updateOnDuplicate = true): int|bool
    {
        // verify table
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        // verify params
        if (empty($rows)) throw new EmptyParamsException("Cannot insert empty data into table.");
        if (!is_array(current($rows))) $rows = [$rows];

        $values = [];
        $params = [];
        $update = [];
        $columns = array_keys(current($rows));

        // populate values and params arrays
        foreach ($rows as $i => $row) {
            if ($columns != array_keys($row)) {
                throw new MismatchedColumnNamesException(array1: $columns, array2: array_keys($row));
            }

            $vals = [];

            foreach ($row as $column => $value) {
                if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
                $params["{$column}_$i"] = $value;
                $vals[] = ":{$column}_$i";
            }

            $values[] = implode(", ", $vals);
        }

        // populate updates array
        foreach ($columns as $column) $update[] = "`$table`.`$column` = `new`.`$column`";

        // convert arrays to strings for use in query
        $columns = implode("`, `", $columns);
        $values = implode(" ), ( ", $values);
        $update = ($updateOnDuplicate) ? "AS `new` ON DUPLICATE KEY UPDATE " . implode(", ", $update) : "";

        // aggregate and execute query
        $sql = "INSERT INTO `$table` ( `$columns` ) VALUES ( $values ) $update";
        if (!$this->query_execute($sql, $params)) return false;

        return $this->get_row_count();
    }


    /**
     * Updates rows in `$table` with `$params` that match `$where` condition(s)
     *
     * @param string $table
     * @param array $updates
     * @param array $where
     * @param string $conjunction
     * @return int|bool
     * @throws RandomException
     */
    public function update(string $table, array $updates, array $where = [], string $conjunction = "AND"): int|bool
    {
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        // generate set clause
        $setClause = [];
        $setParams = [];
        foreach ($updates as $column => $value) {
            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
            $setClause[] = "`$column` = :$column";
            $setParams[$column] = $value;
        }
        $setClause = ($setParams) ? implode(", ", $setClause) : "";

        // generate where clause
        $whereClause = [];
        $whereParams = [];
        foreach ($where as $column => $value) {
            list($column, $operator) = $this::parse_where_operator($column);
            
            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
            
            $whereKey = "where_$column";
            if (in_array($whereKey, $setParams) or $this->valid_schema($table, $whereKey)) {
                // basic key matches an actual column, so we need to generate one that likely isn't a real column
                while (in_array($whereKey, $setParams) or $this->valid_schema($table, $whereKey)) {
                    $whereKey = "where_{$column}_" . bin2hex(random_bytes(10));
                }
            }
            
            $whereClause[] = "`$column` $operator :$whereKey";
            $whereParams[$whereKey] = $value;
        }
        $whereClause = ($whereParams) 
            ? "WHERE " . implode($this::validate_conjunction($conjunction), $whereClause) : "";

        // aggregate and execute query
        $sql = (!$whereClause) ? "UPDATE `$table` SET $setClause;" : "UPDATE `$table` SET $setClause $whereClause;";
        if (!$this->query_execute($sql, array_merge($setParams, $whereParams))) return false;

        return $this->get_row_count();
    }


    /**
     * Selects all rows in `$table` or only ones that match `$where` conditions; `$where` is an array where the key is
     * the column name followed by an optional operator (which defaults to `=`) and the value is the comparison value.
     *
     * @param string $table
     * @param array $where keys can include any of the following operators with the column name: "=", ">", "<", ">=",
     * "<=", "<>", "!=", "BETWEEN", "LIKE", "IN"
     * @param string $conjunction can be "AND" or "OR", case-insensitive
     * @param int $start
     * @param int $limit
     * @return array|bool
     */
    public function select(string $table, array $where = [], string $conjunction = "AND", int $start = 0,
                           int    $limit = 0): array|bool
    {
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        // generate where clause
        $whereClause = [];
        $params = [];
        foreach ($where as $column => $value) {
            list($column, $operator) = $this::parse_where_operator($column);
            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
            $whereClause[] = "`$column` $operator :$column";
            $params[$column] = $value;
        }
        $whereClause = ($params) ? "WHERE " . implode($this::validate_conjunction($conjunction), $whereClause) : "";

        // generate limit clause
        $limitClause = ($limit) ? ($start) ? "LIMIT $start, $limit" : "LIMIT $limit" : "";

        // execute and get results
        if (!$this->query_execute("SELECT * FROM `$table` $whereClause $limitClause;", $params)) return false;
        return $this->fetch_all_results();
    }


    /**
     * Deletes rows that match `$where` conditions
     *
     * @param string $table
     * @param array $where
     * @param string $conjunction
     * @param bool $deleteAll
     * @return int|bool
     */
    public function delete(string $table, array $where, string $conjunction = "AND", bool $deleteAll = false): int|bool
    {
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        // delete all is true, so where conditions don't matter
        if (!$where and $deleteAll) {
            if (!$this->truncate_table($table)) return false;
            return $this->get_row_count();
        }

        // generate where clause
        $whereClause = [];
        $params = [];
        foreach ($where as $column => $value) {
            list($column, $operator) = $this::parse_where_operator($column);
            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
            $whereClause[] = "`$column` $operator :$column";
            $params[$column] = $value;
        }
        $whereClause = ($params) ? "WHERE " . implode($this::validate_conjunction($conjunction), $whereClause) : "";

        if (!$this->query_execute("DELETE FROM $table $whereClause;", $params)) return false;

        return $this->get_row_count();
    }


    /**
     * Transactions
     *  _____                               _   _
     * |_   _| __ __ _ _ __  ___  __ _  ___| |_(_) ___  _ __  ___
     *   | || '__/ _` | '_ \/ __|/ _` |/ __| __| |/ _ \| '_ \/ __|
     *   | || | | (_| | | | \__ \ (_| | (__| |_| | (_) | | | \__ \
     *   |_||_|  \__,_|_| |_|___/\__,_|\___|\__|_|\___/|_| |_|___/
     *
     */
    /**
     * Use a single query to perform an incremental transaction
     *
     * @param string $query
     * @param array $params
     * @param bool $commit
     * @param bool $close
     * @return array
     */
    public function transaction(string $query, array $params, bool $commit = false, bool $close = false): array
    {
        try {
            // start transaction if not already in progress
            if (!$this->pdo->inTransaction()) {
                $this->pdo->beginTransaction();
                if (!$this->pdo->inTransaction()) {
                    // couldn't start a transaction
                    throw new TransactionBeginFailedException("Failed to begin new transaction.");
                }
            }

            // perform single query for the transaction
            if ($this->execute($query, $params)) {
                $results = [
                    'rows' => $this->fetch_all_results(),
                    'row_count' => $this->get_row_count(),
                    'last_id' => $this->get_last_insert_id(),
                ];
            }

            // commit the transaction and return any results
            if (true === $commit) {
                if ($this->pdo->commit()) {
                    // commit the transaction and return the results
                    return $results;
                } else {
                    throw new TransactionCommitFailedException("Failed to commit transaction.");
                }
            } else {
                // return the query results but leave transaction in progress
                return $results;
            }
        } catch (Exception $e) {
            // oh no! roll back database and re-throw whatever fun error was encountered
            if (!$this->rollback()) {
                // we're really not having a good day today are we...
                throw new TransactionRollbackFailedException($e->getMessage() . " -- AND -- Failed to rollback database transaction.");
            }
            throw new TransactionException($e->getMessage());
        }
    }


    /**
     * Pass an array of SQL queries and perform a transaction with them
     *
     * @param array $queries
     * @return array
     */
    public function transaction_execute(array $queries): array
    {
        try {
            // connect to database
            $this->connect();

            // start transaction if not already in progress
            if ($this->pdo->inTransaction()) {
                throw new TransactionInProgressException("Unable to start new transaction while one is already in progress.");
            }
            $this->pdo->beginTransaction();

            // perform transaction
            $results = [];
            foreach ($queries as $query => $params) {
                // prepare query
                $this->prep($query, $params);

                // bind parameters
                if (!empty($params)) {
                    foreach ($params as $var => $val) {
                        $this->bind($var, $val);
                    }
                }

                if ($this->execute()) {
                    $results[] = [
                        'rows' => $this->fetch_all_results(),
                        'row_count' => $this->get_row_count(),
                        'last_id' => $this->get_last_insert_id(),
                    ];
                }
            }

            // commit the transaction and return any results
            if ($this->pdo->commit()) {
                return $results;
            } else {
                throw new TransactionCommitFailedException("Failed to commit transaction.");
            }
        } catch (Exception $e) {
            // Oh no, we dun goof'd! Roll back database and re-throw the error
            $this->pdo->rollback();
            throw new TransactionException($e->getMessage());
        }
    }


    /**
     * @return bool
     */
    public function rollback(): bool
    {
        if ($this->pdo->inTransaction()) {
            if ($this->pdo->rollback()) {
                return true;
            }
        }
        return false;
    }


    /**
     * Table Manipulation
     *  _____     _     _        __  __             _             _       _   _
     * |_   _|_ _| |__ | | ___  |  \/  | __ _ _ __ (_)_ __  _   _| | __ _| |_(_) ___  _ __
     *   | |/ _` | '_ \| |/ _ \ | |\/| |/ _` | '_ \| | '_ \| | | | |/ _` | __| |/ _ \| '_ \
     *   | | (_| | |_) | |  __/ | |  | | (_| | | | | | |_) | |_| | | (_| | |_| | (_) | | | |
     *   |_|\__,_|_.__/|_|\___| |_|  |_|\__,_|_| |_|_| .__/ \__,_|_|\__,_|\__|_|\___/|_| |_|
     *                                               |_|
     */


    /**
     * Renames table named `$oldTable` to `$newTable`
     *
     * @param string $oldTable
     * @param string $newTable
     * @return bool
     * @throws InvalidTableException
     * @throws DuplicateTableException
     */
    public function rename_table(string $oldTable, string $newTable): bool
    {
        if (!$this->valid_schema($oldTable)) throw new InvalidTableException(table: $oldTable);

        if ($this->valid_schema($newTable)) throw new DuplicateTableException($newTable);

        if (!$newTable = $this::valid_schema_string($newTable)) return false;

        return $this->query_execute("RENAME TABLE `$oldTable` TO `$newTable`;");
    }


    /**
     * Truncates table `$table`
     *
     * @param string $table
     * @return int|bool
     * @throws InvalidTableException
     */
    public function truncate_table(string $table): int|bool
    {
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        return $this->query_execute("TRUNCATE TABLE `$table`;");
    }


    /**
     * Drops table `$table`
     *
     * @param string $table
     * @return bool
     * @throws InvalidTableException
     */
    public function drop_table(string $table): bool
    {
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        return $this->query_execute("DROP TABLE `$table`;");
    }


    /**
     * Schema
     *  ____       _
     * / ___|  ___| |__   ___ _ __ ___   __ _
     * \___ \ / __| '_ \ / _ \ '_ ` _ \ / _` |
     *  ___) | (__| | | |  __/ | | | | | (_| |
     * |____/ \___|_| |_|\___|_| |_| |_|\__,_|
     *
     */


    /**
     * Load table schema
     *
     * @param bool $forceReload
     * @return bool
     */
    public function load_table_schema(bool $forceReload = false): bool
    {
        if ($this->tableSchema and !$forceReload) return true;

        $sql = "SELECT `TABLE_NAME`,`COLUMN_NAME`,`DATA_TYPE`
                FROM `INFORMATION_SCHEMA`.`COLUMNS`
                WHERE `TABLE_SCHEMA` = :database_name;";

        if (!$this->query_execute($sql, ['database_name' => $this->name])) {
            return false;
        }

        foreach ($this->fetch_all_results() as $row) {
            $this->tableSchema[$row['TABLE_NAME']][$row['COLUMN_NAME']] = $row['DATA_TYPE'];
        }

        return true;
    }


    /**
     * Load trigger schema
     *
     * @param bool $forceReload
     * @return bool
     */
    public function load_trigger_schema(bool $forceReload = false): bool
    {
        if ($this->triggerSchema and !$forceReload) return true;

        $sql = "SELECT `TRIGGER_NAME`, `EVENT_OBJECT_TABLE`
                FROM `INFORMATION_SCHEMA`.`TRIGGERS`
                WHERE `TRIGGER_SCHEMA` = :database_name;";

        if (!$this->query_execute($sql, ['database_name' => $this->name])) return false;

        foreach ($this->fetch_all_results() as $row) {
            if (!in_array($row['EVENT_OBJECT_TABLE'], $this->triggerSchema)) {
                $this->triggerSchema[$row['EVENT_OBJECT_TABLE']] = [];
            }
            $this->triggerSchema[$row['EVENT_OBJECT_TABLE']][] = $row['TRIGGER_NAME'];
        }

        return true;
    }


    /**
     * Determine if a given table/column combination exists
     * within the database schema
     *
     * @param string $table
     * @param string|null $column
     * @param bool $forceReload
     * @return bool
     */
    public function valid_schema(string $table, string $column = null, bool $forceReload = false): bool
    {
        $this->load_table_schema($forceReload);

        $table = $this::valid_schema_string($table);

        $column = ($column = trim($column ?? "")) ? $this::valid_schema_string($column) : $column;

        // check table
        if (!array_key_exists($table, $this->tableSchema)) return false;

        if (empty($column)) return true;

        // check column
        return array_key_exists($column, $this->tableSchema[$table]);
    }


    /**
     * Determine if a trigger exists within a given table
     *
     * @param string $table
     * @param string $trigger
     * @param bool $forceReload
     * @return bool
     */
    public function valid_trigger(string $table, string $trigger, bool $forceReload = false): bool
    {
        $this->load_trigger_schema($forceReload);

        if (!$this->valid_schema(table: $table)) return false;

        if (in_array(needle: $trigger, haystack: $this->triggerSchema[$table] ?? [])) return true;

        // reload in case schema has changed since last load
        $this->load_trigger_schema();
        return in_array(needle: $trigger, haystack: $this->triggerSchema[$table] ?? []);
    }


    static public function valid_schema_string(string $string): string
    {
        if (!preg_match(pattern: "/^\w+$/i", subject: trim($string), matches: $matches)) {
            throw new InvalidSchemaSyntaxException($string);
        }

        return $matches[0];
    }


    /**
     * Get primary key for a given table within the database
     *
     * @param string $table
     * @return string
     */
    public function table_primary_key(string $table): string
    {
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        $sql = "SHOW KEYS FROM `$table` WHERE `Key_name` = 'PRIMARY';";

        // return primary key
        if (!$this->query_execute($sql)) return "";
        return $this->fetch_first_result()["Column_name"];
    }


    /**
     * Settings
     *  ____       _   _   _
     * / ___|  ___| |_| |_(_)_ __   __ _ ___
     * \___ \ / _ \ __| __| | '_ \ / _` / __|
     *  ___) |  __/ |_| |_| | | | | (_| \__ \
     * |____/ \___|\__|\__|_|_| |_|\__, |___/
     *                             |___/
     */


    /**
     * Creates settings table
     *
     * @return bool
     */
    protected function create_class_table_nestbox_settings(): bool
    {
        $sql = "CREATE TABLE IF NOT EXISTS `nestbox_settings` (
                    `package_name` VARCHAR( 64 ) NOT NULL ,
                    `setting_name` VARCHAR( 64 ) NOT NULL ,
                    `setting_type` VARCHAR( 64 ) NOT NULL ,
                    `setting_value` VARCHAR( 128 ) NULL ,
                    PRIMARY KEY ( `setting_name` )
                ) ENGINE = InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=utf8mb4_general_ci;";

        return $this->query_execute($sql);
    }


    /**
     * Loads settings from the settings table
     *
     * @return array
     */
    public function load_settings(): array
    {
        $where = ['package_name' => self::PACKAGE_NAME];

        try {
            $settings = $this->parse_settings($this->select(table: $this::nestbox_settings_table, where: $where));
        } catch (InvalidTableException) {
            var_dump("creating settings table", $this->create_class_table_nestbox_settings());
            $this->load_table_schema();
            $this->parse_settings($this->select(table: $this::nestbox_settings_table, where: $where));
        }


        foreach ($settings as $name => $value) {
            if (property_exists($this, $name)) {
                $this->update_setting($name, $value);
            }
        }

        return $settings;
    }


    /**
     * Updates setting $name with value $value
     *
     * @param string $name
     * @param string $value
     * @return bool
     */
    public function update_setting(string $name, string $value): bool
    {
        if (!property_exists($this, $name)) return false;

        $this->$name = $value;

        return true;
    }


    /**
     * Saves current settings to the settings table
     *
     * @return void
     */
    public function save_settings(): void
    {
        $sql = "INSERT INTO `nestbox_settings` (
                    `package_name`, `setting_name`, `setting_type`, `setting_value`
                ) VALUES (
                    :package_name, :setting_name, :setting_type, :setting_value
                ) ON DUPLICATE KEY UPDATE
                    `package_name` = :package_name,
                    `setting_name` = :setting_name,
                    `setting_type` = :setting_type,
                    `setting_value` = :setting_value;";

        foreach (get_class_vars(get_class($this)) as $name => $value) {
            if (!str_starts_with($name, needle: self::PACKAGE_NAME)) {
                continue;
            }

            $params = [
                "package_name" => self::PACKAGE_NAME,
                "setting_name" => $name,
                "setting_type" => $this->parse_setting_type($value),
                "setting_value" => strval($value),
            ];

            $this->query_execute($sql, $params);
        }
    }


    /**
     * Parses string setting values and converts them from string to detected types
     *
     * @param array $settings
     * @return array
     */
    protected function parse_settings(array $settings): array
    {
        $output = [];
        foreach ($settings as $setting) {
            $output[$setting['setting_name']] = $this->setting_type_conversion(type: $setting['setting_type'], value: $setting['setting_value']);
        }
        return $output;
    }


    /**
     * Detects the variable type of $setting
     *
     * @param int|float|bool|array|string $setting
     * @return string
     */
    protected function parse_setting_type(int|float|bool|array|string $setting): string
    {
        if (is_int($setting)) return "string";
        if (is_float($setting)) return "float";
        if (is_bool($setting)) return "boolean";
        if (is_array($setting)) return "array";
        if (json_validate($setting)) return "json";
        return "string";
    }


    /**
     * Converts and returns $value into type defined by $type
     *
     * @param string $type
     * @param string $value
     * @return int|float|bool|array|string
     */
    protected function setting_type_conversion(string $type, string $value): int|float|bool|array|string
    {
        if ("int" == strtolower($type)) {
            return intval($value);
        }

        if (in_array(strtolower($type), ["double", "float"])) {
            return floatval($value);
        }

        if ("bool" == strtolower($type)) {
            return boolval($value);
        }

        if (in_array(strtolower($type), ["array", "json"])) {
            return json_decode($value, associative: true);
        }

        return $value;
    }


    /**
     * Error Logging
     *  _____                       _                      _
     * | ____|_ __ _ __ ___  _ __  | |    ___   __ _  __ _(_)_ __   __ _
     * |  _| | '__| '__/ _ \| '__| | |   / _ \ / _` |/ _` | | '_ \ / _` |
     * | |___| |  | | | (_) | |    | |__| (_) | (_| | (_| | | | | | (_| |
     * |_____|_|  |_|  \___/|_|    |_____\___/ \__, |\__, |_|_| |_|\__, |
     *                                         |___/ |___/         |___/
     */


    /**
     * Creates the logging table
     *
     * @return void
     */
    protected function create_class_table_nestbox_errors(): void
    {
        $sql = "CREATE TABLE IF NOT EXISTS `nestbox_errors` (
                    `error_id` INT NOT NULL AUTO_INCREMENT ,
                    `occurred` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
                    `message` VARCHAR( 512 ) NOT NULL ,
                    `query` VARCHAR( 4096 ) NOT NULL ,
                    PRIMARY KEY ( `error_id` )
                ) ENGINE = InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=utf8mb4_general_ci;";
        $this->query_execute($sql);
    }


    /**
     * Logs an error
     *
     * @param string $message
     * @param string $query
     * @return int
     */
    protected function log_error(string $message, string $query): int
    {
        $error = [
            "message" => substr(string: $message, offset: 0, length: 512),
            "query" => substr(string: $query, offset: 0, length: 4096),
        ];
        return $this->insert(table: 'nestbox_errors', rows: $error);
    }


    /**
     * Import/Export
     *  ___                            _      _______                       _
     * |_ _|_ __ ___  _ __   ___  _ __| |_   / / ____|_  ___ __   ___  _ __| |_
     *  | || '_ ` _ \| '_ \ / _ \| '__| __| / /|  _| \ \/ / '_ \ / _ \| '__| __|
     *  | || | | | | | |_) | (_) | |  | |_ / / | |___ >  <| |_) | (_) | |  | |_
     * |___|_| |_| |_| .__/ \___/|_|   \__/_/  |_____/_/\_\ .__/ \___/|_|   \__|
     *               |_|                                  |_|
     */


    /**
     * Fetches all rows from `$table` and returns them as a JSON string
     *
     * @param string $table
     * @return string|false
     */
    public function dump_table(string $table): string|false
    {
        return json_encode($this->select($table));
    }


    /**
     * Fetches all rows from all tables, or tables named in `$tables`, and returns them as a JSON string
     *
     * @param array $tables
     * @return string|false
     */
    public function dump_database(array $tables = []): string|false
    {
        $this->load_table_schema();

        if (empty($tables)) $tables = array_keys($this->tableSchema);

        $output = [];

        foreach ($tables as $table) $output[$table] = $this->select($table);

        return json_encode($output);
    }


    /**
     * Takes a JSON string of table rows, inserts the table data, and returns the number of rows inserted; if
     * `$truncate` is set to `true` the tables will be truncated before insert
     *
     * @param string $table
     * @param string|array $data
     * @param bool $truncate
     * @return int
     */
    public function load_table(string $table, string|array $data, bool $truncate = false): int
    {
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        if (is_string($data)) {
            if (!json_validate($data)) throw new MalformedJsonException;

            $data = json_decode($data, associative: true);
        }

        if ($truncate) $this->truncate_table($table);

        return $this->insert(table: $table, rows: $data);
    }


    /**
     * Takes a JSON string of tables and rows, inserts the table data, and returns the number of rows inserted; if
     * `$truncate` is set to `true` the tables will be truncated before insert
     *
     * @param string|array $input
     * @param bool $truncate
     * @return int
     */
    public function load_database(string|array $input, bool $truncate = false): int
    {
        $updateCount = 0;

        if (is_string($input)) {
            if (!json_validate($input)) throw new MalformedJsonException;

            $input = json_decode($input, associative: true);
        }

        foreach ($input as $table => $data) {
            $updateCount += $this->load_table(table: $table, data: $data, truncate: $truncate);
        }

        return $updateCount;
    }
}
