<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox;

use NestboxPHP\Nestbox\Exception\FailedToPrepareStatement;
use NestboxPHP\Nestbox\Exception\ResultFetchException;
use NestboxPHP\Nestbox\Exception\TransactionImplicitCommitNotAllowed;
use NestboxPHP\Nestbox\Exception\TransactionNotInProgressException;
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
    public const PACKAGE_NAME = 'nestbox';
    public const NESTBOX_SETTINGS_TABLE = 'nestbox_settings';
    public const NESTBOX_ERROR_TABLE = 'nestbox_errors';
    final protected const NESTBOX_DIRECTORY = "../nestbox";

    // connection properties
    protected string $host = 'localhost';
    protected string $user = 'root';
    protected string $pass = '';
    protected string $name = '';

    protected PDO $pdo;
    protected PDOStatement $stmt;

    protected array $tableSchema = [];
    protected array $triggerSchema = [];
    protected array $generatedColumns = [];

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
        $this->save_settings(static::PACKAGE_NAME);

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
        $this->save_settings(static::PACKAGE_NAME);

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
     * Calls all class methods that start with `create_class_table_` and refreshes `$this->tableSchema`
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
     * Takes a string or array of folder names, verifies path separators, validates the provided path relative to the
     * `$_SERVER["DOCUMENT_ROOT"]` directory by prepending it if it isn't already present, then finally returns the
     * generated string
     *
     * @param string|array $path
     * @return string
     */
    protected function generate_document_root_relative_path(string|array $path): string
    {
        $path = (is_array($path)) ? implode(separator: DIRECTORY_SEPARATOR, array: $path) : $path;

        $path = (!str_contains(haystack: $path, needle: $_SERVER["DOCUMENT_ROOT"]))
            ? implode("/", [$_SERVER["DOCUMENT_ROOT"], $path]) : $path;

        return trim(
            string: preg_replace(pattern: '#[/\\\\]+#', replacement: DIRECTORY_SEPARATOR, subject: $path),
            characters: DIRECTORY_SEPARATOR
        );
    }


    /**
     * Uses `generate_document_root_relative_path()` to validate the `$path` input, then recursively creates the given
     * directory path with the defined permissions
     *
     * @param string|array $path
     * @param int $permissions
     * @return bool
     */
    protected function create_document_root_relative_directory(string|array $path, int $permissions): bool
    {
        if (is_array($path) or !str_contains(haystack: $path, needle: $_SERVER["DOCUMENT_ROOT"]))
            $path = $this->generate_document_root_relative_path($path);

        if (!file_exists($path))
            return mkdir(directory: $path, permissions: $permissions, recursive: true);

        return chmod(filename: $path, permissions: $permissions);
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
     * Creates a new connection to the database or uses the current one if it exists
     *
     * @return true
     * @throws NestboxException
     */
    public function connect(): true
    {
        // check for existing connection
        if ($this->check_connection()) return true;

        // MySQL Database
        try {
            $this->pdo = new PDO(
                dsn: "mysql:host=$this->host;dbname=$this->name",
                username: $this->user,
                password: $this->pass,
                options: [
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                    PDO::ATTR_PERSISTENT => true,
                    PDO::ATTR_EMULATE_PREPARES => true, // off for :named placeholders
                    PDO::ATTR_STRINGIFY_FETCHES => false,
                ]
            );
        } catch (PDOException $e) {
            // re-throw as NestboxException for class catching
            throw new NestboxException($e->getMessage(), $e->getCode(), $e->getPrevious());
        }

        // successful connection
        return true;
    }


    /**
     * Returns `true` if a connection exists and has not timed out, otherwise `false`
     *
     * @return bool
     */
    public function check_connection(): bool
    {
        if (!empty($this->pdo)) {
            // test existing connection for possible timeout
            $this->prep("SELECT 1");
            $this->execute();
            $rows = $this->fetch_all_results();

            // check test results
            if (1 === $rows[0]['1']) return true;

            // kill dead or timed-out connection
            $this->close();
        }

        return false;
    }


    /**
     * Closes an existing connection
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
     * @param array $params Array of key=>value pairs for :named parameters
     * @param bool $close Close the connection upon query execution
     * @return true
     * @throws EmptyQueryException
     */
    public function query_execute(string $query, array $params = [], bool $close = false): true
    {
        // check query emptiness
        if (empty(trim($query))) throw new EmptyQueryException("Cannot execute an empty query.");

        // connect to database
        $this->connect();

        // prepare statement and bind parameters
        $params = $this::validate_parameters($query, $params);
        $this->prep($query);
        if ($params) foreach ($params as $col => $val) $this->bind($col, $val);

        // execute, close if applicable, and return results
        $this->execute();
        if ($close) $this->close();
        return true;
    }


    /**
     * Prepare a query statement
     *
     * @param string $query
     * @return true
     * @throws FailedToPrepareStatement
     */
    protected function prep(string $query): true
    {
        // prepare the query statement
        if (!$this->stmt = $this->pdo->prepare($query))
            throw new FailedToPrepareStatement($this->parse_pdo_error_info("Failed to prepare"));
        return true;
    }


    /**
     * Bind a variable to a named parameter
     *
     * @param $variable
     * @param $value
     * @return true
     * @throws InvalidParameterValueType
     * @throws FailedToBindValueException
     */
    protected function bind($variable, $value): true
    {
        // set binding type
        $type = Nestbox::get_parameter_pdo_type($value);
        if (false === $type) throw new InvalidParameterValueType(variable: $variable, value: $value);

        // backwards compatibility or whatever
        $variable = (!str_starts_with($variable, ':')) ? ":$variable" : $variable;

        // bind value to parameter
        if (true === $this->stmt->bindValue($variable, $value, $type)) return true;

        // we didn't do it
        throw new FailedToBindValueException($this->parse_statement_error_info("Failed to bind"));
    }


    /**
     * Checks variable type for parameter binding
     *
     * @param $parameter
     * @return int|bool
     */
    public static function get_parameter_pdo_type($parameter): int|bool
    {
        return match (gettype($parameter)) {
            "boolean" => PDO::PARAM_BOOL,
            "integer" => PDO::PARAM_INT,
            "double", "string" => PDO::PARAM_STR,
            "NULL" => PDO::PARAM_NULL,
            default => false
        };
    }


    /**
     * Execute a statement or throws either a QueryErrorException or a PDOException
     *
     * @return true
     * @throws QueryErrorException
     * @throws PDOException
     */
    protected function execute(): true
    {
        // execute query
        try {
            if ($this->stmt->execute()) return true;
            throw new QueryErrorException($this->parse_statement_error_info(prefix: "MySQL error"));
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
     * @return array [`"param1"` => `$val1`, `"param2"` => `$val2`...]
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
        if ($queryParams) throw new MissingParametersException(json_encode($queryParams));

        return $output;
    }


    /**
     * Returns "ASC" or "DESC" based on input string, case-insensitive, otherwise "ASC" for invalid input
     *
     * @param string $order
     * @return string `"ASC"` | `"DESC"`
     */
    public static function validate_order(string $order): string
    {
        return (in_array(strtoupper($order), ["ASC", "DESC"])) ? strtoupper($order) : "ASC";
    }


    /**
     * Ensures the conjunction provided for *WHERE* clause is *AND* or *OR*, otherwise returns *AND*
     *
     * @param string $conjunction
     * @return string `"AND"` | `"OR"`
     */
    public static function validate_conjunction(string $conjunction): string
    {
        return (preg_match("/^(and|or)$/i", trim($conjunction), $con)) ? $con[0] : "AND";
    }


    /**
     * Generates an array of column names from a given list of parameters by using the keys of the first element in a
     * multidimensional array, or the keys of all elements in a flat array
     *
     * @param array $params
     * @return array [`"col1"`, `"col2"`...]
     */
    public static function generate_column_list_from_params(array $params): array
    {
        return (is_array(current($params))) ? array_keys(current($params)) : array_keys($params);
    }


    /**
     * Generates a VALUES clause with it's associated parameters in an array
     * @param string $table
     * @param array $values
     * @return array [`"VALUES :col1, :col2"`.., [`"col1"` => `$val1`, `"col2"` => `$val2`...]]
     * @throws InvalidColumnException
     */
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
     * Takes a parameter array of *[`"col_name"` => `$colVal`]* parameters and generates the as-new-on-duplicate-key-
     * update clause with update strings of `"table.col1 = new.col1"` separated by commas, and the associated parameters
     * array to use for binding the update values to the generated clause
     *
     * @param string $table
     * @param array $updates
     * @return array [`"AS...KEY UPDATE..."`, [`"col1"` => `$val1`, `"col2"` => `$val2`...]]
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
     * Generates the set clause with it's associated array of parameters to use for binding
     *
     * @param string $table
     * @param array $updates
     * @return array [`"SET col1 = :col1..."`, [`"col1"` => `$val1`...]]
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
     * Takes an array of parameters and generates a string of named parameters unique for each row to be inserted,
     * separated by commas, and their associated parameters array to be used for binding
     *
     * @param array $params
     * @return array [`"(:col1_0, :col2_0), (:col1_1, :col2_1)..."`, [`"col1_0"` => `$val_1`,... `"col2_1"` => `$val_x`...]
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
     * Generates a where clause with it's associated array of parameters for binding, or an empty string if `$where` has
     * no values; each key in the `$where` parameter must be a valid column and may include a space and any of following
     * operators: `=` *(default)*, `>`, `<`, `>=`, `<=`, `<>`, `!=`, `BETWEEN`, `LIKE`, `IN`
     *
     * @param string $table
     * @param array $where [`"col"` => `$val`] pairs with optional space and operator appended to column name
     * @param string $conjunction can be `"AND"` *(default)* or `"OR"`, case-insensitive
     * @return array [`"WHERE col1 = :col1 [AND|OR...]"`, [`"col1"` => `$col1`...]]
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
     * Generates a limit clause depending on whether one or both `$start` and `$limit` parameters are greater than `0`,
     * otherwise returns an empty string
     *
     * @param int $start
     * @param int $limit
     * @return string `"LIMIT $start, $limit"` | `"LIMIT $limit"` | `""`
     */
    public static function generate_limit_clause(int $start = 0, int $limit = 0): string
    {
        if ($limit) return ($start) ? "LIMIT $start, $limit" : "LIMIT $limit";
        return "";
    }


    /**
     * Takes the key string from a `$where` parameter elsewhere and returns an array with the column name and any
     * included operator, defaulting to `"="` if no operator is present; the column names are __NOT__ validated against
     * any table schema and should be verified elsewhere
     *
     * @param string $columnWithOperator
     * @return array [`"col"`, `"="`...]
     * @throws InvalidWhereOperator
     */
    protected static function parse_where_operator(string $columnWithOperator): array
    {
        $pattern = "/^`?(\w+)`?\s+([<!=>]{1,2}|between|like|in|is(\snot)?)$/i";
        //                                                         ^lol

        // if no operator exists, default to "="
        if (!preg_match($pattern, trim($columnWithOperator), $matches)) {
            $matches = [$columnWithOperator, self::valid_schema_string($columnWithOperator), "="];
        }

        $validOperators = ["=", ">", "<", ">=", "<=", "<>", "!=", "BETWEEN", "LIKE", "IN", "IS", "IS NOT"];

        // make sure the operator is valid, such as proper order of <, !, =, and > characters
        if (!in_array(strtoupper($matches[2]), $validOperators))
            throw new InvalidWhereOperator($matches[2]);

        // return column name and the operator
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
     * @param int $fetchMode `PDO::FETCH_ASSOC`
     * @return array
     * @throws ResultFetchException
     */
    public function fetch_all_results(int $fetchMode = PDO::FETCH_ASSOC): array
    {
        $results = $this->stmt->fetchAll($fetchMode);
        if (false === $results)
            throw new ResultFetchException($this->parse_statement_error_info("Fetch all error"));
        return $results;
    }


    /**
     * Fetch first row from result set
     *
     * @param int $fetchMode `PDO::FETCH_ASSOC`
     * @return array
     * @throws ResultFetchException
     */
    public function fetch_first_result(int $fetchMode = PDO::FETCH_ASSOC): array
    {
        $results = $this->stmt->fetchAll($fetchMode);
        if (false === $results)
            throw new ResultFetchException($this->parse_statement_error_info("Fetch first error"));
        return $results[0] ?? [];
    }


    /**
     * Fetch next result in a set
     *
     * @param int $fetchMode `PDO::FETCH_ASSOC`
     * @return array
     * @throws ResultFetchException
     */
    public function fetch_next_result(int $fetchMode = PDO::FETCH_ASSOC): array
    {
        $result = $this->stmt->fetch($fetchMode);
        if (false === $result)
            throw new ResultFetchException($this->parse_statement_error_info("Fetch next error"));
        return $result;
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
     * Inserts one or more rows of data, updating existing rows based on primary key; `$rows` may be a single array with
     * column names and values to insert, or a multidimensional array where each child array is the row to insert.
     *
     * @param string $table
     * @param array $rows [`"col1"` => `$val1`] | [`0` => [`"col1"` => `$val1`...]...]
     * @param bool $updateOnDuplicate `true`
     * @return int|bool
     * @throws EmptyParamsException
     * @throws MismatchedColumnNamesException
     * @throws InvalidColumnException
     * @throws InvalidTableException
     */
    public function insert(string $table, array $rows, bool $updateOnDuplicate = true): int|bool
    {
        /**
         * TODO: A note for myself: I could take out the MismatchedColumnNamesException if I pre-loaded the default
         * TODO: columns and set them to null or an empty string depending on the table structure requirements, but this
         * TODO: would assume the insert array was created properly and wouldn't take into account when typos or user
         * TODO: error decides to join the party, as is typically the case. Options to be thought over.
         *
         * example:
         * $defaultColumns = array_combine(
         *     keys: array_keys($this->tableSchema[$tableName]),
         *     values: array_fill(0, count($this->tableSchema[$tableName]), "")
         * );
         */
        // verify table
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        // verify params
        if (empty($rows)) throw new EmptyParamsException("Cannot insert empty data into table $table.");
        if (!is_array(current($rows))) $rows = [$rows];

        $values = [];
        $params = [];
        $update = [];
        $columns = [];

        // populate columns array
        foreach (current($rows) as $column => $value) {
            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
            if ($this->is_generated_column($table, $column)) continue;
            $columns[] = $column;
        }

        // populate values and params arrays
        foreach ($rows as $i => $row) {
            $vals = [];

            foreach ($row as $column => $value) {
                if ($this->is_generated_column($table, $column)) continue;
                if (!in_array($column, $columns)) {
                    throw new MismatchedColumnNamesException(array1: $columns, array2: array_keys($row));
                }

                $paramName = preg_replace("/\s/", "_", "{$column}_$i");
                $params[$paramName] = $value;
                $vals[] = ":$paramName";
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
     */
    public function update(string $table, array $updates, array $where = [], string $conjunction = "AND"): int|bool
    {
        /**
         * TODO: It's possible to update multiple rows at once using the following style of query by identifying the
         * TODO: table's primary key and using it in tandem with a case statement, although this does seem exceptionally
         * TODO: convoluted and could take some significant work to implement. Second- and third-order effects would
         * TODO: include a more robust implementation of the where operator parser and the where clause generator to
         * TODO: handle arrays of values. Food for thought, I guess.
         *
         * UPDATE my_table
         * SET value_to_update =
         *     CASE
         *         WHEN id = 1 THEN 'New Value 1'
         *         WHEN id = 2 THEN 'New Value 2'
         *         WHEN id = 3 THEN 'New Value 3'
         *         ELSE value_to_update  -- Keep existing value for other rows
         *     END
         * WHERE id IN (1, 2, 3);
         */
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        // generate set clause
        $setClause = [];
        $setParams = [];
        foreach ($updates as $column => $value) {
            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
            // generated columns are valid schema but cannot be set
            if ($this->is_generated_column($table, $column)) continue;
            if ($this->table_primary_key($table) == $column) {
                if (in_array($column, $where))
                    throw new NestboxException("Can't set primary key while it is also in the where clause.");
                $where[$column] = $value;
                continue;
            }

            $paramName = preg_replace("/\s/", "_", $column);
            $setClause[] = "`$column` = :$paramName";
            $setParams[$paramName] = $value;
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
                    // technically this could loop forever, but the likelihood is astronomical
                    try {
                        $whereKey = "where_{$column}_" . bin2hex(random_bytes(10));
                    } catch (RandomException $e) {
                        // re-throw as NestboxException for class catching
                        throw new NestboxException($e->getMessage(), $e->getCode(), $e->getPrevious());
                    }
                }
            }

            $whereClause[] = "`$column` $operator :$whereKey";
            $whereParams[$whereKey] = $value;
        }
        $whereClause = (!$whereParams) ? ""
            : "WHERE " . implode(" " . $this::validate_conjunction($conjunction) . " ", $whereClause);

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
     * @param array $orderBy
     * @return array|bool
     */
    public function select(string $table, array $where = [], string $conjunction = "AND", int $start = 0,
                           int    $limit = 0, array $orderBy = []): array|bool
    {
        if (!$this->valid_schema($table)) throw new InvalidTableException(table: $table);

        // generate select clause
        $selectClause = "SELECT * FROM `$table`";

        // generate where clause
        list($whereClause, $params) = $this->generate_where_clause($table, $where, $conjunction);

        // generate order by clause
        $orderByClause = [];
        foreach ($orderBy as $column => $order) {
            if (!$this->valid_schema($table, $column)) throw new InvalidColumnException(table: $table, column: $column);
            $orderByClause[] = $column . " " . $this::validate_order($order);
        }
        $orderByClause = ($orderByClause) ? "ORDER BY " . implode(", ", $orderByClause) : "";

        // generate limit clause
        $limitClause = static::generate_limit_clause($start, $limit);

        // execute and get results
        if (!$this->query_execute("$selectClause $whereClause $orderByClause $limitClause;", $params)) {
            return false;
        }
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
        list($whereClause, $params) = $this->generate_where_clause($table, $where, $conjunction);

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
     * Starts a new transaction
     *
     * @return true
     * @throws TransactionInProgressException
     * @throws TransactionBeginFailedException
     */
    public function transaction_start(): true
    {
        $this->connect();

        if ($this->pdo->inTransaction())
            throw new TransactionInProgressException("Can't start a new transaction while one is in progress.");

        if (!$this->pdo->beginTransaction()) {
            $errorInfo = $this->pdo->errorInfo();
            $error = "Failed to begin new transaction [$errorInfo[0]]: $errorInfo[2] ($errorInfo[1])";
            throw new TransactionBeginFailedException($error);
        }

        return true;
    }


    /**
     * Increments a transaction with a query
     *
     * @param string $query
     * @param array|null $params
     * @return array
     * @throws TransactionNotInProgressException
     */
    public function transaction_increment(string $query, array|null $params = null): array
    {
        $implicitCommitOccurred = false;

        if (!$this->pdo->inTransaction())
            throw new TransactionNotInProgressException("No transaction in progress to increment.");

        $this->prep($query);

        if (is_array($params)) {
            foreach ($params as $variable => $value) {
                $this->bind($variable, $value);
            }
        }

        if (!$this->execute()) {
            return [
                "rows" => 0,
                "row_count" => 0,
                "last_id" => "",
                "implicit_commit_occurred" => false,
                "success" => false,
                "error_info" => $this->pdo->errorInfo()
            ];
        }

        if (!$this->pdo->inTransaction()) {
            $implicitCommitOccurred = true;
            $this->transaction_start();
        }

        return [
            "rows" => $this->fetch_all_results(),
            "row_count" => $this->get_row_count(),
            "last_id" => $this->get_last_insert_id(),
            "implicit_commit_occurred" => $implicitCommitOccurred,
            "success" => true,
            "error_info" => ["", "", ""]
        ];
    }


    /**
     * Rollback the current transaction
     *
     * @return true
     * @throws TransactionRollbackFailedException
     */
    public function transaction_rollback(): true
    {
        if (!$this->pdo->inTransaction())
            throw new TransactionNotInProgressException("Cannot rollback a transaction not in progress.");

        if (!$this->pdo->rollback())
            throw new TransactionRollbackFailedException("Failed to rollback transaction.");

        return true;
    }


    /**
     * Commits transaction changes to database
     *
     * @return true
     * @throws TransactionNotInProgressException
     * @throws TransactionCommitFailedException
     */
    public function transaction_commit(): true
    {
        if (!$this->pdo->inTransaction())
            throw new TransactionNotInProgressException("Cannot commit a transaction not in progress.");

        if (!$this->pdo->commit())
            throw new TransactionCommitFailedException("Failed to commit transaction.");

        return true;
    }


    /**
     * Detects if a query would trigger an implicit commit as defined by the MySQL documentation:
     * https://dev.mysql.com/doc/refman/8.0/en/implicit-commit.html
     *
     * @param string $query
     * @return string|false
     */
    public function transaction_implicit_commit_detection(string $query): string|false
    {
        // Split a query
        $subqueries = array_filter(explode(";", $query));

        // Data definition language statements that define or modify database objects
        $ddl = [
            "/^ALTER (EVENT|FUNCTION|PROCEDURE|SERVER|TABLE|TABLESPACE|VIEW).*$/i",
            "/^CREATE (DATABASE|EVENT|FUNCTION|INDEX|PROCEDURE|ROLE|SERVER|SPATIAL REFERENCE SYSTEM|TABLE|TABLESPACE|TRIGGER|VIEW).*$/i",
            "/^DROP (DATABASE|EVENT|FUNCTION|INDEX|PROCEDURE|ROLE|SERVER|SPATIAL REFERENCE SYSTEM|TABLE|TABLESPACE|TRIGGER|VIEW).*$/i",
            "/^(INSTALL|UNINSTALL) PLUGIN.*$/i",
            "/^(RENAME|TRUNCATE) TABLE.*$/i",
        ];

        foreach ($subqueries as $query) {
            foreach ($ddl as $pattern) {
                if (preg_match($pattern, $query))
                    return "Data definition language: $query";
            }
        }

        // Statements that implicitly use or modify the tables in the mysql database
        $mysql = [
            "/^(ALTER|CREATE|DROP|RENAME) USER.*$/i",
            "/^GRANT.*$/i",
            "/^REVOKE.*$/i",
            "/^SET PASSWORD.*$/i",
        ];

        foreach ($subqueries as $query) {
            foreach ($mysql as $pattern) {
                if (preg_match($pattern, $query))
                    return "MySQL database usage or modification: $query";
            }
        }

        // Transaction-control and locking statements
        $tcls = [
            "/^BEGIN.*$/i",
            "/^LOCK TABLES.*$/i",
            "/^SET autocommit = 1.*$/i",
            "/^START TRANSACTION.*$/i",
            "/^UNLOCK TABLES.*$/i"
        ];

        foreach ($subqueries as $query) {
            foreach ($tcls as $pattern) {
                if (preg_match($pattern, $query))
                    return "Transaction-control or locking statement: $query";
            }
        }

        // Data loading statements
        $data = [
            "/^LOAD DATA.*$/i"
        ];

        foreach ($subqueries as $query) {
            foreach ($data as $pattern) {
                if (preg_match($pattern, $query))
                    return "Data loading statement: $query";
            }
        }

        // Administrative statements
        $admin = [
            "/^ANALYZE TABLE.*$/i",
            "/^CACHE INDEX.*$/i",
            "/^(CHECK|OPTIMIZE|REPAIR) TABLE.*$/i",
            "/^FLUSH.*$/i",
            "/^LOAD INDEX INTO CACHE.*$/i",
            "/^RESET (?!PERSIST).*$/i"
        ];

        foreach ($subqueries as $query) {
            foreach ($admin as $pattern) {
                if (preg_match($pattern, $query))
                    return "Administrative statement: $query";
            }
        }

        // Replication control statements
        $rcs = [
            "/^(START|STOP|RESET) REPLICA.*$/i",
            "/^CHANGE (REPLICATION SOURCE|MASTER) TO.*$/i"
        ];

        foreach ($subqueries as $query) {
            foreach ($rcs as $pattern) {
                if (preg_match($pattern, $query))
                    return "Replication control statement: $query";
            }
        }

        return false;
    }


    /**
     * Pass an array of SQL queries, where the key is the query and the value is an array of parameters (or empty/null)
     * and perform a transaction with them
     *
     * @param array $queries
     * @param bool $commit
     * @param bool $rollbackOnFail
     * @param bool $allowImplicitCommits
     * @return array|false
     * @throws TransactionImplicitCommitNotAllowed
     */
    public function transaction_execute(array $queries, bool $commit = false, bool $rollbackOnFail = true,
                                        bool $allowImplicitCommits = false): array|false
    {
        $results = [];

        $this->transaction_start();

        foreach ($queries as $query => $params) {
            if (true !== $allowImplicitCommits) {
                $implicitCommit = $this->transaction_implicit_commit_detection($query);

                if ($implicitCommit)
                    throw new TransactionImplicitCommitNotAllowed($implicitCommit);
            }

            $result = $this->transaction_increment($query, $params);

            if (!$result and $rollbackOnFail) {
                $this->transaction_rollback();
                return false;
            }

            $results[] = $result;
        }

        if (true === $commit) {
            $this->transaction_commit();
        } else {
            $this->transaction_rollback();
        }

        return $results;
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

        $sql = "SELECT `TABLE_NAME`,`COLUMN_NAME`,`DATA_TYPE`,`GENERATION_EXPRESSION`
                FROM `INFORMATION_SCHEMA`.`COLUMNS`
                WHERE `TABLE_SCHEMA` = :database_name;";

        if (!$this->query_execute($sql, ['database_name' => $this->name])) {
            return false;
        }

        foreach ($this->fetch_all_results() as $row) {
            $this->tableSchema[$row['TABLE_NAME']][$row['COLUMN_NAME']] = $row['DATA_TYPE'];
            if ($row['GENERATION_EXPRESSION']) $this->generatedColumns[$row['TABLE_NAME']][] = $row['COLUMN_NAME'];
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
        if (!preg_match(pattern: "/^[\w\s]+$/i", subject: trim($string), matches: $matches)) {
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


    public function table_schema(): array
    {
        return $this->tableSchema;
    }

    public function is_generated_column(string $table, string $column): bool
    {
        return in_array($column, $this->generatedColumns[$table] ?? []);
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
        $sql = "CREATE TABLE IF NOT EXISTS `" . Nestbox::NESTBOX_SETTINGS_TABLE . "` (
                    `package_name` VARCHAR( 64 ) NOT NULL ,
                    `setting_name` VARCHAR( 64 ) NOT NULL ,
                    `setting_type` VARCHAR( 64 ) NOT NULL ,
                    `setting_value` VARCHAR( 128 ) NULL ,
                    PRIMARY KEY ( `setting_name` )
                ) ENGINE = InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=utf8mb4_general_ci;";

        return $this->query_execute($sql);
    }


    public function is_setting(string $settingName): bool
    {
        return (bool)$this->get_setting($settingName);
    }


    public function get_setting(string $settingName): array
    {
        $setting = $this->select(table: static::NESTBOX_SETTINGS_TABLE, where: ["setting_name" => $settingName]);

        return $this->parse_settings($setting);
    }


    public function get_settings(string $package = null): array
    {
        $where = ($package) ? ['package_name' => $package] : [];

        $settings = $this->select(table: $this::NESTBOX_SETTINGS_TABLE, where: $where);

        return $this->parse_settings($settings);
    }


    /**
     * Loads settings from the settings table
     *
     * @return array
     */
    public function load_settings(): array
    {
        $settings = $this->get_settings(static::PACKAGE_NAME);

        foreach ($settings as $name => $value) {
            if (!property_exists($this, $name)) continue;

            $type = $this->parse_setting_type($value);
            $this->$name = $this->setting_type_conversion($type, $value);
        }

        return $settings;
    }


    /**
     * Updates setting $name with value $value
     *
     * @param string $name
     * @param string $value
     * @return int|bool
     */
    public function update_setting(string $name, mixed $value): int|bool
    {
        if (!$this->is_setting($name)) return false;

        $row = [
            "setting_name" => $name,
            "setting_value" => $value
        ];

        $updateCount = $this->update(static::NESTBOX_SETTINGS_TABLE, $row);
        if (false === $updateCount) return false;
        $this->load_settings();
        return $updateCount;
    }


    public function update_settings(array $settings): int|bool
    {
        $updateCount = 0;

        foreach ($settings as $settingName => $settingValue) {
            if (!$this->is_setting($settingName)) continue;

            $update = [
                "setting_name" => $settingName,
                "setting_value" => $settingValue
            ];

            $updateCount += $this->update(static::NESTBOX_SETTINGS_TABLE, $update);
        }

        $this->load_settings();
        return $updateCount;
    }


    /**
     * Saves current settings to the settings table
     *
     * @param string|null $packageName
     * @return int|bool
     */
    public function save_settings(string $packageName = null): int|bool
    {
        $settings = [];

        $packageName = ($packageName) ?: static::PACKAGE_NAME;

        foreach (get_class_vars(get_class($this)) as $name => $value) {
            if (!str_starts_with($name, needle: $packageName)) continue;

            $settings[] = [
                "package_name" => static::PACKAGE_NAME,
                "setting_name" => $name,
                "setting_type" => $this->parse_setting_type($value),
                "setting_value" => strval($value),
            ];

        }

        return ($settings) ? $this->insert($this::NESTBOX_SETTINGS_TABLE, $settings) : false;
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
            $type = $setting['setting_type'];
            $value = $setting['setting_value'];
            $parsed = $this->setting_type_conversion($type, $value);
            $output[$setting['setting_name']] = $parsed;
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
        if (is_int($setting)) return "integer";
        if (is_float($setting)) return "float";
        if (is_bool($setting)) return "boolean";
        if (is_array($setting)) return "array";
        if (json_decode($setting)) return "json";
        return "string";
    }


    /**
     * Converts and returns `$value` into type defined by `$type`
     *
     * @param string $type
     * @param int|float|bool|array|string $value
     * @return int|float|bool|array|string
     */
    protected function setting_type_conversion(string $type, int|float|bool|array|string $value): int|float|bool|array|string
    {
        if (in_array(strtolower($type), ["integer", "int"])) {
            return intval($value);
        }

        if (in_array(strtolower($type), ["double", "float"])) {
            return floatval($value);
        }

        if (in_array(strtolower($type), ["bool", "boolean"])) {
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
        $sql = "CREATE TABLE IF NOT EXISTS `" . static::NESTBOX_ERROR_TABLE . "` (
                    `error_id` INT NOT NULL AUTO_INCREMENT ,
                    `occurred` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ,
                    `message` VARCHAR( 1024 ) NOT NULL ,
                    `request` VARCHAR( 4096 ) NOT NULL ,
                    `details` VARCHAR( 4096 ) NOT NULL ,
                    PRIMARY KEY ( `error_id` )
                ) ENGINE = InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=utf8mb4_general_ci;";
        $this->query_execute($sql);
    }


    /**
     * Logs an error
     *
     * @param string $message
     * @param string $request
     * @param string $details
     * @return int
     */
    public function log_error(string $message, string $request, string $details): int
    {
        $error = [
            "message" => substr(string: $message, offset: 0, length: 512),
            "request" => substr(string: $request, offset: 0, length: 4096),
            "details" => substr(string: $details, offset: 0, length: 4096),
        ];
        return $this->insert(table: static::NESTBOX_ERROR_TABLE, rows: $error);
    }


    public function parse_pdo_error_info(string $prefix): string
    {
        $error = $this->pdo->errorInfo();
        return trim("$prefix [$error[0]]: $error[2] ($error[1])");
    }


    public function parse_statement_error_info(string $prefix): string
    {
        $error = $this->stmt->errorInfo();
        return trim("$prefix [$error[0]]: $error[2] ($error[1])");
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

        if (is_string($data)) if (!$data = json_decode($data, associative: true)) throw new MalformedJsonException;

        if ($truncate) $this->truncate_table($table);

        return $this->insert(table: $table, rows: $data);
    }


    /**
     * Takes a JSON string of tables and rows, inserts the table data, and returns the number of rows inserted; if
     * `$truncate` is set to `true`, the tables will be truncated before insert and cannot be rolled back if attempted
     * during a transaction
     *
     * @param string|array $input
     * @param bool $truncate
     * @return array
     */
    public function load_database(string|array $input, bool $truncate = false): array
    {
        $updateCount = 0;
        $errors = [];

        if (is_string($input)) if (!$input = json_decode($input, associative: true)) throw new MalformedJsonException;

        foreach ($input as $table => $data) {
            try {
                $updateCount += $this->load_table(table: $table, data: $data, truncate: $truncate);
            } catch (EmptyParamsException $e) {
                $errors[] = $e->getMessage();
            }
        }

        return [$updateCount, $errors];
    }

    protected function create_import_queue_directory(): true
    {
        return true;
    }

    protected function save_json_in_queue_directory(): true
    {
        return true;
    }

    protected function process_queue(): true
    {
        // load queue files
        // get start time
        // get script timeout seconds
        // loop through files
        // get table name
        // calculate seconds per row
        // get row count
        // estimate time of insert
        // return if estimate is over script timeout
        // if more than 3000 rows, row chunk
        // insert rows
        return true;
    }
}
