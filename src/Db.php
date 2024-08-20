<?php

declare(strict_types=1);

namespace Cleup\Core\Database;

use PDO;
use Exception;
use PDOException;
use PDOStatement;
use InvalidArgumentException;

class Db
{
    /**
     * Library Version
     * 
     * @var string 
     */
    public const VERSION = '1.0.1';

    /**
     * Compatibility of the core version of the framework
     * 
     * @var string 
     */
    public const CLEUP_VERSION = '1.0.1';

    /**
     * The PDO object.
     *
     * @var \PDO
     */
    public static $pdo;

    /**
     * The type of database.
     *
     * @var string
     */
    public static $type;

    /**
     * Table prefix.
     *
     * @var string
     */
    protected static $prefix;

    /**
     * The PDO statement object.
     *
     * @var \PDOStatement
     */
    protected static $statement;

    /**
     * The DSN connection string.
     *
     * @var string
     */
    protected static $dsn;

    /**
     * The array of logs.
     *
     * @var array
     */
    protected static $logs = [];

    /**
     * Determine should log the query or not.
     *
     * @var bool
     */
    protected static $logging = false;

    /**
     * Determine is in test mode.
     *
     * @var bool
     */
    protected static $testMode = false;

    /**
     * The last query string was generated in test mode.
     *
     * @var string
     */
    public static $queryString;

    /**
     * Determine is in debug mode.
     *
     * @var bool
     */
    protected static $debugMode = false;

    /**
     * Determine should save debug logging.
     *
     * @var bool
     */
    protected static $debugLogging = false;

    /**
     * The array of logs for debugging.
     *
     * @var array
     */
    protected static $debugLogs = [];

    /**
     * The unique global id.
     *
     * @var integer
     */
    protected static $guid = 0;

    /**
     * The returned id for the insert.
     *
     * @var string
     */
    public static $returnId = '';

    /**
     * Error Message.
     *
     * @var string|null
     */
    public static $error = null;

    /**
     * The array of error information.
     *
     * @var array|null
     */
    public static $errorInfo = null;

    /**
     * @param array $options Connection options
     * @return Db
     * @throws PDOException
     * @codeCoverageIgnore
     */

    public static function config(array $options)
    {
        if (isset($options['prefix'])) {
            static::$prefix = $options['prefix'];
        }

        if (isset($options['testMode']) && $options['testMode'] == true) {
            static::$testMode = true;
            return;
        }

        $options['type'] = $options['type'] ?? $options['database_type'];

        if (!isset($options['pdo'])) {
            $options['database'] = $options['database'] ?? $options['database_name'];

            if (!isset($options['socket'])) {
                $options['host'] = $options['host'] ?? $options['server'] ?? false;
            }
        }

        if (isset($options['type'])) {
            static::$type = strtolower($options['type']);

            if (static::$type === 'mariadb') {
                static::$type = 'mysql';
            }
        }

        if (isset($options['logging']) && is_bool($options['logging'])) {
            static::$logging = $options['logging'];
        }

        $option = $options['option'] ?? [];
        $commands = [];

        switch (static::$type) {

            case 'mysql':
                // Make MySQL using standard quoted identifier.
                $commands[] = 'SET SQL_MODE=ANSI_QUOTES';

                break;

            case 'mssql':
                // Keep MSSQL QUOTED_IDENTIFIER is ON for standard quoting.
                $commands[] = 'SET QUOTED_IDENTIFIER ON';

                // Make ANSI_NULLS is ON for NULL value.
                $commands[] = 'SET ANSI_NULLS ON';

                break;
        }

        if (isset($options['pdo'])) {
            if (!$options['pdo'] instanceof PDO) {
                throw new InvalidArgumentException('Invalid PDO object supplied.');
            }

            static::$pdo = $options['pdo'];

            foreach ($commands as $value) {
                static::$pdo->exec($value);
            }

            return;
        }

        if (isset($options['dsn'])) {
            if (is_array($options['dsn']) && isset($options['dsn']['driver'])) {
                $attr = $options['dsn'];
            } else {
                throw new InvalidArgumentException('Invalid DSN option supplied.');
            }
        } else {
            if (
                isset($options['port']) &&
                is_int($options['port'] * 1)
            ) {
                $port = $options['port'];
            }

            $isPort = isset($port);

            switch (static::$type) {

                case 'mysql':
                    $attr = [
                        'driver' => 'mysql',
                        'dbname' => $options['database']
                    ];

                    if (isset($options['socket'])) {
                        $attr['unix_socket'] = $options['socket'];
                    } else {
                        $attr['host'] = $options['host'];

                        if ($isPort) {
                            $attr['port'] = $port;
                        }
                    }

                    break;

                case 'pgsql':
                    $attr = [
                        'driver' => 'pgsql',
                        'host' => $options['host'],
                        'dbname' => $options['database']
                    ];

                    if ($isPort) {
                        $attr['port'] = $port;
                    }

                    break;

                case 'sybase':
                    $attr = [
                        'driver' => 'dblib',
                        'host' => $options['host'],
                        'dbname' => $options['database']
                    ];

                    if ($isPort) {
                        $attr['port'] = $port;
                    }

                    break;

                case 'oracle':
                    $attr = [
                        'driver' => 'oci',
                        'dbname' => $options['host'] ?
                            '//' . $options['host'] . ($isPort ? ':' . $port : ':1521') . '/' . $options['database'] :
                            $options['database']
                    ];

                    if (isset($options['charset'])) {
                        $attr['charset'] = $options['charset'];
                    }

                    break;

                case 'mssql':
                    if (isset($options['driver']) && $options['driver'] === 'dblib') {
                        $attr = [
                            'driver' => 'dblib',
                            'host' => $options['host'] . ($isPort ? ':' . $port : ''),
                            'dbname' => $options['database']
                        ];

                        if (isset($options['appname'])) {
                            $attr['appname'] = $options['appname'];
                        }

                        if (isset($options['charset'])) {
                            $attr['charset'] = $options['charset'];
                        }
                    } else {
                        $attr = [
                            'driver' => 'sqlsrv',
                            'Server' => $options['host'] . ($isPort ? ',' . $port : ''),
                            'Database' => $options['database']
                        ];

                        if (isset($options['appname'])) {
                            $attr['APP'] = $options['appname'];
                        }

                        $config = [
                            'ApplicationIntent',
                            'AttachDBFileName',
                            'Authentication',
                            'ColumnEncryption',
                            'ConnectionPooling',
                            'Encrypt',
                            'Failover_Partner',
                            'KeyStoreAuthentication',
                            'KeyStorePrincipalId',
                            'KeyStoreSecret',
                            'LoginTimeout',
                            'MultipleActiveResultSets',
                            'MultiSubnetFailover',
                            'Scrollable',
                            'TraceFile',
                            'TraceOn',
                            'TransactionIsolation',
                            'TransparentNetworkIPResolution',
                            'TrustServerCertificate',
                            'WSID',
                        ];

                        foreach ($config as $value) {
                            $keyname = strtolower(preg_replace(['/([a-z\d])([A-Z])/', '/([^_])([A-Z][a-z])/'], '$1_$2', $value));

                            if (isset($options[$keyname])) {
                                $attr[$value] = $options[$keyname];
                            }
                        }
                    }

                    break;

                case 'sqlite':
                    $attr = [
                        'driver' => 'sqlite',
                        $options['database']
                    ];

                    break;
            }
        }

        if (!isset($attr)) {
            throw new InvalidArgumentException('Incorrect connection options.');
        }

        $driver = $attr['driver'];

        if (!in_array($driver, PDO::getAvailableDrivers())) {
            throw new InvalidArgumentException("Unsupported PDO driver: {$driver}.");
        }

        unset($attr['driver']);

        $stack = [];

        foreach ($attr as $key => $value) {
            $stack[] = is_int($key) ? $value : $key . '=' . $value;
        }

        $dsn = $driver . ':' . implode(';', $stack);

        if (
            in_array(static::$type, ['mysql', 'pgsql', 'sybase', 'mssql']) &&
            isset($options['charset'])
        ) {
            $commands[] = "SET NAMES '{$options['charset']}'" . (static::$type === 'mysql' && isset($options['collation']) ?
                " COLLATE '{$options['collation']}'" : ''
            );
        }

        static::$dsn = $dsn;

        try {
            static::$pdo = new PDO(
                $dsn,
                $options['username'] ?? null,
                $options['password'] ?? null,
                $option
            );

            if (isset($options['error'])) {
                static::$pdo->setAttribute(
                    PDO::ATTR_ERRMODE,
                    in_array($options['error'], [
                        PDO::ERRMODE_SILENT,
                        PDO::ERRMODE_WARNING,
                        PDO::ERRMODE_EXCEPTION
                    ]) ?
                        $options['error'] :
                        PDO::ERRMODE_SILENT
                );
            }

            if (isset($options['command']) && is_array($options['command'])) {
                $commands = array_merge($commands, $options['command']);
            }

            foreach ($commands as $value) {
                static::$pdo->exec($value);
            }
        } catch (PDOException $e) {
            throw new PDOException($e->getMessage());
        }
    }

    /**
     * Generate a new map key for the placeholder.
     *
     * @return string
     */
    protected static function mapKey(): string
    {
        return ':Db' . static::$guid++ . '_pe';
    }

    /**
     * Execute customized raw statement.
     *
     * @param string $statement The raw SQL statement.
     * @param array $map The array of input parameters value for prepared statement.
     * @return \PDOStatement|null
     */
    public static function query(string $statement, array $map = [],  array $ultraMap = []): ?PDOStatement
    {
        $raw = static::raw($statement, $map);
        $statement = static::buildRaw($raw, $map);

        if ($ultraMap)
            $map = $ultraMap;

        return static::exec($statement, $map);
    }


    /**
     * Prepare data using the map
     *
     * @param array Map
     */

    public static function prepareMapData($map = array()): array
    {
        $r = [];

        foreach ($map as $key => $val) {
            if (isset($val[0]))
                $r[$key] = $val[0];
        }

        return $r;
    }


    /**
     * Execute the raw statement.
     *
     * @param string $statement The SQL statement.
     * @param array $map The array of input parameters value for prepared statement.
     * @codeCoverageIgnore
     * @return \PDOStatement|null
     */
    public static function exec(string $statement, array $map = [], callable $callback = null): ?PDOStatement
    {
        static::$statement = null;
        static::$errorInfo = null;
        static::$error = null;

        if (static::$testMode) {
            static::$queryString = static::generate($statement, $map);
            return null;
        }

        if (static::$debugMode) {
            if (static::$debugLogging) {
                static::$debugLogs[] = static::generate($statement, $map);
                return null;
            }

            echo static::generate($statement, $map);

            static::$debugMode = false;

            return null;
        }

        if (static::$logging) {
            static::$logs[] = [$statement, $map];
        } else {
            static::$logs = [[$statement, $map]];
        }

        $statement = static::$pdo->prepare($statement);
        $errorInfo = static::$pdo->errorInfo();

        if ($errorInfo[0] !== '00000') {
            static::$errorInfo = $errorInfo;
            static::$error = $errorInfo[2];

            return null;
        }

        foreach ($map as $key => $value) {
            $statement->bindValue($key, $value[0], $value[1]);
        }

        if (is_callable($callback)) {
            static::$pdo->beginTransaction();
            $callback($statement);
            $execute = $statement->execute();
            static::$pdo->commit();
        } else {
            $execute = $statement->execute();
        }

        $errorInfo = $statement->errorInfo();

        if ($errorInfo[0] !== '00000') {
            static::$errorInfo = $errorInfo;
            static::$error = $errorInfo[2];

            return null;
        }

        if ($execute) {
            static::$statement = $statement;
        }

        return $statement;
    }

    /**
     * Generate readable statement.
     *
     * @param string $statement
     * @param array $map
     * @codeCoverageIgnore
     * @return string
     */
    protected static function generate(string $statement, array $map): string
    {
        $identifier = [
            'mysql' => '`$1`',
            'mssql' => '[$1]'
        ];

        $statement = preg_replace(
            '/(?!\'[^\s]+\s?)"([\p{L}_][\p{L}\p{N}@$#\-_]*)"(?!\s?[^\s]+\')/u',
            $identifier[static::$type] ?? '"$1"',
            $statement
        );

        foreach ($map as $key => $value) {
            if ($value[1] === PDO::PARAM_STR) {
                $replace = static::quote("{$value[0]}");
            } elseif ($value[1] === PDO::PARAM_NULL) {
                $replace = 'NULL';
            } elseif ($value[1] === PDO::PARAM_LOB) {
                $replace = '{LOB_DATA}';
            } else {
                $replace = $value[0] . '';
            }

            $statement = str_replace($key, $replace, $statement);
        }

        return $statement;
    }

    /**
     * Build a raw object.
     *
     * @param string $string The raw string.
     * @param array $map The array of mapping data for the raw string.
     * @return Raw
     */
    public static function raw(string $string, array $map = []): Raw
    {
        $raw = new Raw();
        $raw->map = $map;
        $raw->value = $string;

        return $raw;
    }

    /**
     * Finds whether the object is raw.
     *
     * @param object $object
     * @return bool
     */
    protected static function isRaw($object): bool
    {
        return $object instanceof Raw;
    }

    /**
     * Generate the actual query from the raw object.
     *
     * @param mixed $raw
     * @param array $map
     * @return string|null
     */
    protected static function buildRaw($raw, array &$map): ?string
    {
        if (!static::isRaw($raw)) {
            return null;
        }

        $query = preg_replace_callback(
            '/(([`\'])[\<]*?)?((FROM|TABLE|INTO|UPDATE|JOIN|TABLE IF EXISTS)\s*)?\<(([\p{L}_][\p{L}\p{N}@$#\-_]*)(\.[\p{L}_][\p{L}\p{N}@$#\-_]*)?)\>([^,]*?\2)?/',
            function ($matches) {
                if (!empty($matches[2]) && isset($matches[8])) {
                    return $matches[0];
                }

                if (!empty($matches[4])) {
                    return $matches[1] . $matches[4] . ' ' . static::tableQuote($matches[5]);
                }

                return $matches[1] . static::columnQuote($matches[5]);
            },
            $raw->value
        );

        $rawMap = $raw->map;

        if (!empty($rawMap)) {
            foreach ($rawMap as $key => $value) {
                $map[$key] = static::typeMap($value, gettype($value));
            }
        }

        return $query;
    }

    /**
     * Quote a string for use in a query.
     *
     * @param string $string
     * @return string
     */
    public static function quote(string $string): string
    {
        if (static::$type === 'mysql') {
            return "'" . preg_replace(['/([\'"])/', '/(\\\\\\\")/'], ["\\\\\${1}", '\\\${1}'], $string) . "'";
        }

        return "'" . preg_replace('/\'/', '\'\'', $string) . "'";
    }

    /**
     * Quote table name for use in a query.
     *
     * @param string $table
     * @return string
     */
    public static function tableQuote(string $table): string
    {
        if (preg_match('/^[\p{L}_][\p{L}\p{N}@$#\-_]*$/u', $table)) {
            return '"' . static::$prefix . $table . '"';
        }

        throw new InvalidArgumentException("Incorrect table name: {$table}.");
    }

    /**
     * Quote column name for use in a query.
     *
     * @param string $column
     * @return string
     */
    public static function columnQuote(string $column): string
    {
        if (preg_match('/^[\p{L}_][\p{L}\p{N}@$#\-_]*(\.?[\p{L}_][\p{L}\p{N}@$#\-_]*)?$/u', $column)) {
            return strpos($column, '.') !== false ?
                '"' . static::$prefix . str_replace('.', '"."', $column) . '"' :
                '"' . $column . '"';
        }

        throw new InvalidArgumentException("Incorrect column name: {$column}.");
    }

    /**
     * Mapping the type name as PDO data type.
     *
     * @param mixed $value
     * @param string $type
     * @return array
     */
    protected static function typeMap($value, string $type): array
    {
        $map = [
            'NULL' => PDO::PARAM_NULL,
            'integer' => PDO::PARAM_INT,
            'double' => PDO::PARAM_STR,
            'boolean' => PDO::PARAM_BOOL,
            'string' => PDO::PARAM_STR,
            'object' => PDO::PARAM_STR,
            'resource' => PDO::PARAM_LOB
        ];

        if ($type === 'boolean') {
            $value = ($value ? '1' : '0');
        } elseif ($type === 'NULL') {
            $value = null;
        }

        return [$value, $map[$type]];
    }

    /**
     * Build the statement part for the column stack.
     *
     * @param array|string $columns
     * @param array $map
     * @param bool $root
     * @param bool $isJoin
     * @return string
     */
    protected static function columnPush(&$columns, array &$map, bool $root, bool $isJoin = false): string
    {
        if ($columns === '*') {
            return $columns;
        }

        $stack = [];
        $hasDistinct = false;

        if (is_string($columns)) {
            $columns = [$columns];
        }

        foreach ($columns as $key => $value) {
            $isIntKey = is_int($key);
            $isArrayValue = is_array($value);

            if (!$isIntKey && $isArrayValue && $root && count(array_keys($columns)) === 1) {
                $stack[] = static::columnQuote($key);
                $stack[] = static::columnPush($value, $map, false, $isJoin);
            } elseif ($isArrayValue) {
                $stack[] = static::columnPush($value, $map, false, $isJoin);
            } elseif (!$isIntKey && $raw = static::buildRaw($value, $map)) {
                preg_match('/(?<column>[\p{L}_][\p{L}\p{N}@$#\-_\.]*)(\s*\[(?<type>(String|Bool|Int|Number))\])?/u', $key, $match);
                $colQuoted = static::columnQuote($match['column']);
                $stack[] = "{$raw} AS {$colQuoted}";
            } elseif ($isIntKey && is_string($value)) {
                if ($isJoin && strpos($value, '*') !== false) {
                    throw new InvalidArgumentException('Cannot use table.* to select all columns while joining table.');
                }

                preg_match('/(?<column>[\p{L}_][\p{L}\p{N}@$#\-_\.]*)(?:\s*\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\))?(?:\s*\[(?<type>(?:String|Bool|Int|Number|Object|JSON))\])?/u', $value, $match);

                $columnString = '';

                if (!empty($match['alias'])) {
                    $colQuoted = static::columnQuote($match['column']);
                    $aliasQuoted = static::columnQuote($match['alias']);
                    $columnString = "{$colQuoted} AS {$aliasQuoted}";
                    $columns[$key] = $match['alias'];

                    if (!empty($match['type'])) {
                        $columns[$key] .= ' [' . $match['type'] . ']';
                    }
                } else {
                    $columnString = static::columnQuote($match['column']);
                }

                if (!$hasDistinct && strpos($value, '@') === 0) {
                    $columnString = 'DISTINCT ' . $columnString;
                    $hasDistinct = true;
                    array_unshift($stack, $columnString);

                    continue;
                }

                $stack[] = $columnString;
            }
        }

        return implode(',', $stack);
    }

    /**
     * Implode the Where conditions.
     *
     * @param array $data
     * @param array $map
     * @param string $conjunctor
     * @return string
     */
    protected static function dataImplode(array $data, array &$map, string $conjunctor): string
    {
        $stack = [];

        foreach ($data as $key => $value) {
            $type = gettype($value);

            if (
                $type === 'array' &&
                preg_match("/^(AND|OR)(\s+#.*)?$/", $key, $relationMatch)
            ) {
                $stack[] = '(' . static::dataImplode($value, $map, ' ' . $relationMatch[1]) . ')';
                continue;
            }

            $mapKey = static::mapKey();
            $isIndex = is_int($key);

            preg_match(
                '/([\p{L}_][\p{L}\p{N}@$#\-_\.]*)(\[(?<operator>.*)\])?([\p{L}_][\p{L}\p{N}@$#\-_\.]*)?/u',
                $isIndex ? $value : $key,
                $match
            );

            $column = static::columnQuote($match[1]);
            $operator = $match['operator'] ?? null;

            if ($isIndex && isset($match[4]) && in_array($operator, ['>', '>=', '<', '<=', '=', '!='])) {
                $stack[] = "{$column} {$operator} " . static::columnQuote($match[4]);
                continue;
            }

            if ($operator && $operator != '=') {
                if (in_array($operator, ['>', '>=', '<', '<='])) {
                    $condition = "{$column} {$operator} ";

                    if (is_numeric($value)) {
                        $condition .= $mapKey;
                        $map[$mapKey] = [$value, is_float($value) ? PDO::PARAM_STR : PDO::PARAM_INT];
                    } elseif ($raw = static::buildRaw($value, $map)) {
                        $condition .= $raw;
                    } else {
                        $condition .= $mapKey;
                        $map[$mapKey] = [$value, PDO::PARAM_STR];
                    }

                    $stack[] = $condition;
                } elseif ($operator === '!') {
                    switch ($type) {

                        case 'NULL':
                            $stack[] = $column . ' IS NOT NULL';
                            break;

                        case 'array':
                            $values = [];

                            foreach ($value as $index => $item) {
                                if ($raw = static::buildRaw($item, $map)) {
                                    $values[] = $raw;
                                } else {
                                    $stackKey = $mapKey . $index . '_i';

                                    $values[] = $stackKey;
                                    $map[$stackKey] = static::typeMap($item, gettype($item));
                                }
                            }

                            $stack[] = $column . ' NOT IN (' . implode(', ', $values) . ')';
                            break;

                        case 'object':
                            if ($raw = static::buildRaw($value, $map)) {
                                $stack[] = "{$column} != {$raw}";
                            }
                            break;

                        case 'integer':
                        case 'double':
                        case 'boolean':
                        case 'string':
                            $stack[] = "{$column} != {$mapKey}";
                            $map[$mapKey] = static::typeMap($value, $type);
                            break;
                    }
                } elseif ($operator === '~' || $operator === '!~') {
                    if ($type !== 'array') {
                        $value = [$value];
                    }

                    $connector = ' OR ';
                    $data = array_values($value);

                    if (is_array($data[0])) {
                        if (isset($value['AND']) || isset($value['OR'])) {
                            $connector = ' ' . array_keys($value)[0] . ' ';
                            $value = $data[0];
                        }
                    }

                    $likeClauses = [];

                    foreach ($value as $index => $item) {
                        $likeKey = "{$mapKey}_{$index}_i";
                        $item = strval($item);

                        if (!preg_match('/((?<!\\\)\[.+(?<!\\\)\]|(?<!\\\)[\*\?\!\%#^_]|%.+|.+%)/', $item)) {
                            $item = '%' . $item . '%';
                        }

                        $likeClauses[] = $column . ($operator === '!~' ? ' NOT' : '') . " LIKE {$likeKey}";
                        $map[$likeKey] = [$item, PDO::PARAM_STR];
                    }

                    $stack[] = '(' . implode($connector, $likeClauses) . ')';
                } elseif ($operator === '<>' || $operator === '><') {
                    if ($type === 'array') {
                        if ($operator === '><') {
                            $column .= ' NOT';
                        }

                        if (static::isRaw($value[0]) && static::isRaw($value[1])) {
                            $stack[] = "({$column} BETWEEN {" . static::buildRaw($value[0], $map) . "} AND {" . static::buildRaw($value[1], $map) . "})";
                        } else {
                            $stack[] = "({$column} BETWEEN {$mapKey}a AND {$mapKey}b)";
                            $dataType = (is_numeric($value[0]) && is_numeric($value[1])) ? PDO::PARAM_INT : PDO::PARAM_STR;

                            $map[$mapKey . 'a'] = [$value[0], $dataType];
                            $map[$mapKey . 'b'] = [$value[1], $dataType];
                        }
                    }
                } elseif ($operator === 'REGEXP') {
                    $stack[] = "{$column} REGEXP {$mapKey}";
                    $map[$mapKey] = [$value, PDO::PARAM_STR];
                } else {
                    throw new InvalidArgumentException("Invalid operator [{$operator}] for column {$column} supplied.");
                }

                continue;
            }

            switch ($type) {

                case 'NULL':
                    $stack[] = $column . ' IS NULL';
                    break;

                case 'array':
                    $values = [];

                    foreach ($value as $index => $item) {
                        if ($raw = static::buildRaw($item, $map)) {
                            $values[] = $raw;
                        } else {
                            $stackKey = $mapKey . $index . '_i';

                            $values[] = $stackKey;
                            $map[$stackKey] = static::typeMap($item, gettype($item));
                        }
                    }

                    $stack[] = $column . ' IN (' . implode(', ', $values) . ')';
                    break;

                case 'object':
                    if ($raw = static::buildRaw($value, $map)) {
                        $stack[] = "{$column} = {$raw}";
                    }
                    break;

                case 'integer':
                case 'double':
                case 'boolean':
                case 'string':
                    $stack[] = "{$column} = {$mapKey}";
                    $map[$mapKey] = static::typeMap($value, $type);
                    break;
            }
        }

        return implode($conjunctor . ' ', $stack);
    }

    /**
     * Build the where clause.
     *
     * @param array|null $where
     * @param array $map
     * @return string
     */
    public static function whereClause($where, array &$map): string
    {
        $clause = '';

        if (is_array($where)) {
            $conditions = array_diff_key($where, array_flip(
                ['GROUP', 'ORDER', 'HAVING', 'LIMIT', 'LIKE', 'MATCH']
            ));

            if (!empty($conditions)) {
                $clause = ' WHERE ' . static::dataImplode($conditions, $map, ' AND');
            }

            if (isset($where['MATCH']) && static::$type === 'mysql') {
                $match = $where['MATCH'];

                if (is_array($match) && isset($match['columns'], $match['keyword'])) {
                    $mode = '';

                    $options = [
                        'natural' => 'IN NATURAL LANGUAGE MODE',
                        'natural+query' => 'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION',
                        'boolean' => 'IN BOOLEAN MODE',
                        'query' => 'WITH QUERY EXPANSION'
                    ];

                    if (isset($match['mode'], $options[$match['mode']])) {
                        $mode = ' ' . $options[$match['mode']];
                    }

                    $cols = [];

                    if (!empty($match['columns'])) {
                        foreach ($match['columns'] as $col) {
                            $cols[] = static::columnQuote($col);
                        }
                    }
                    $columns = implode(', ', $cols);
                    $mapKey = static::mapKey();
                    $map[$mapKey] = [$match['keyword'], PDO::PARAM_STR];
                    $clause .= ($clause !== '' ? ' AND ' : ' WHERE') . ' MATCH (' . $columns . ') AGAINST (' . $mapKey . $mode . ')';
                }
            }

            if (isset($where['GROUP'])) {
                $group = $where['GROUP'];

                if (is_array($group)) {
                    $stack = [];

                    foreach ($group as $column => $value) {
                        $stack[] = static::columnQuote($value);
                    }

                    $clause .= ' GROUP BY ' . implode(',', $stack);
                } elseif ($raw = static::buildRaw($group, $map)) {
                    $clause .= ' GROUP BY ' . $raw;
                } else {
                    $clause .= ' GROUP BY ' . static::columnQuote($group);
                }
            }

            if (isset($where['HAVING'])) {
                $having = $where['HAVING'];

                if ($raw = static::buildRaw($having, $map)) {
                    $clause .= ' HAVING ' . $raw;
                } else {
                    $clause .= ' HAVING ' . static::dataImplode($having, $map, ' AND');
                }
            }

            if (isset($where['ORDER'])) {
                $order = $where['ORDER'];

                if (is_array($order)) {
                    $stack = [];


                    foreach ($order as $column => $value) {
                        if (is_array($value)) {
                            $valueStack = [];

                            foreach ($value as $item) {
                                $valueStack[] = is_int($item) ? $item : static::quote($item);
                            }

                            $valueString = implode(',', $valueStack);
                            $colQuoted = static::columnQuote($column);
                            $stack[] = "FIELD({$colQuoted}, {$valueString})";
                        } elseif ($value === 'ASC' || $value === 'DESC') {
                            $stack[] = static::columnQuote($column) . ' ' . $value;
                        } elseif (is_int($column)) {
                            $stack[] = static::columnQuote($value);
                        }
                    }

                    $clause .= ' ORDER BY ' . implode(',', $stack);
                } elseif ($raw = static::buildRaw($order, $map)) {
                    $clause .= ' ORDER BY ' . $raw;
                } else {
                    $clause .= ' ORDER BY ' . static::columnQuote($order);
                }
            }

            if (isset($where['LIMIT'])) {
                $limit = $where['LIMIT'];

                if (in_array(static::$type, ['oracle', 'mssql'])) {
                    if (static::$type === 'mssql' && !isset($where['ORDER'])) {
                        $clause .= ' ORDER BY (SELECT 0)';
                    }

                    if (is_numeric($limit)) {
                        $limit = [0, $limit];
                    }

                    if (
                        is_array($limit) &&
                        is_numeric($limit[0]) &&
                        is_numeric($limit[1])
                    ) {
                        $clause .= " OFFSET {$limit[0]} ROWS FETCH NEXT {$limit[1]} ROWS ONLY";
                    }
                } else {
                    if (is_numeric($limit)) {
                        $clause .= ' LIMIT ' . $limit;
                    } elseif (
                        is_array($limit) &&
                        is_numeric($limit[0]) &&
                        is_numeric($limit[1])
                    ) {
                        $clause .= " LIMIT {$limit[1]} OFFSET {$limit[0]}";
                    }
                }
            }
        } elseif ($raw = static::buildRaw($where, $map)) {
            $clause .= ' ' . $raw;
        }

        return $clause;
    }

    /**
     * Build statement for the select query.
     *
     * @param string $table
     * @param array $map
     * @param array|string $join
     * @param array|string $columns
     * @param array $where
     * @param string $columnFn
     * @return string
     */
    public static function selectContext(
        string $table,
        array &$map,
        $join,
        &$columns = null,
        $where = null,
        $columnFn = null
    ): string {
        preg_match('/(?<table>[\p{L}_][\p{L}\p{N}@$#\-_]*)\s*\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\)/u', $table, $tableMatch);

        if (isset($tableMatch['table'], $tableMatch['alias'])) {
            $table = static::tableQuote($tableMatch['table']);
            $tableAlias = static::tableQuote($tableMatch['alias']);
            $tableQuery = "{$table} AS {$tableAlias}";
        } else {
            $table = static::tableQuote($table);
            $tableQuery = $table;
        }

        $isJoin = static::isJoin($join);

        if ($isJoin) {
            $tableQuery .= ' ' . static::buildJoin($tableAlias ?? $table, $join, $map);
        } else {
            if (is_null($columns)) {
                if (
                    !is_null($where) ||
                    (is_array($join) && isset($columnFn))
                ) {
                    $where = $join;
                    $columns = null;
                } else {
                    $where = null;
                    $columns = $join;
                }
            } else {
                $where = $columns;
                $columns = $join;
            }
        }

        if (isset($columnFn)) {
            if ($columnFn === 1) {
                $column = '1';

                if (is_null($where)) {
                    $where = $columns;
                }
            } elseif ($raw = static::buildRaw($columnFn, $map)) {
                $column = $raw;
            } else {
                if (empty($columns) || static::isRaw($columns)) {
                    $columns = '*';
                    $where = $join;
                }

                $column = $columnFn . '(' . static::columnPush($columns, $map, true) . ')';
            }
        } else {
            $column = static::columnPush($columns, $map, true, $isJoin);
        }

        return 'SELECT ' . $column . ' FROM ' . $tableQuery . static::whereClause($where, $map);
    }

    /**
     * Determine the array with join syntax.
     *
     * @param mixed $join
     * @return bool
     */
    protected static function isJoin($join): bool
    {
        if (!is_array($join)) {
            return false;
        }

        $keys = array_keys($join);

        if (
            isset($keys[0]) &&
            is_string($keys[0]) &&
            strpos($keys[0], '[') === 0
        ) {
            return true;
        }

        return false;
    }

    /**
     * Build the join statement.
     *
     * @param string $table
     * @param array $join
     * @param array $map
     * @return string
     */
    public static function buildJoin(string $table, array $join, array &$map): string
    {
        $tableJoin = [];
        $type = [
            '>' => 'LEFT',
            '<' => 'RIGHT',
            '<>' => 'FULL',
            '><' => 'INNER'
        ];

        foreach ($join as $subtable => $relation) {
            preg_match('/(\[(?<join>\<\>?|\>\<?)\])?(?<table>[\p{L}_][\p{L}\p{N}@$#\-_]*)\s?(\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\))?/u', $subtable, $match);

            if ($match['join'] === '' || $match['table'] === '') {
                continue;
            }

            if (is_string($relation)) {
                $relation = 'USING ("' . $relation . '")';
            } elseif (is_array($relation)) {
                // For ['column1', 'column2']
                if (isset($relation[0])) {
                    $relation = 'USING ("' . implode('", "', $relation) . '")';
                } else {
                    $joins = [];

                    foreach ($relation as $key => $value) {
                        if ($key === 'AND' && is_array($value)) {
                            $joins[] = static::dataImplode($value, $map, ' AND');
                            continue;
                        }

                        $joins[] = (
                            strpos($key, '.') > 0 ?
                            // For ['tableB.column' => 'column']
                            static::columnQuote($key) :

                            // For ['column1' => 'column2']
                            $table . '.' . static::columnQuote($key)
                        ) .
                            ' = ' .
                            static::tableQuote($match['alias'] ?? $match['table']) . '.' . static::columnQuote($value);
                    }

                    $relation = 'ON ' . implode(' AND ', $joins);
                }
            } elseif ($raw = static::buildRaw($relation, $map)) {
                $relation = $raw;
            }

            $tableName = static::tableQuote($match['table']);

            if (isset($match['alias'])) {
                $tableName .= ' AS ' . static::tableQuote($match['alias']);
            }

            $tableJoin[] = $type[$match['join']] . " JOIN {$tableName} {$relation}";
        }

        return implode(' ', $tableJoin);
    }

    /**
     * Mapping columns for the stack.
     *
     * @param array|string $columns
     * @param array $stack
     * @param bool $root
     * @return array
     */
    protected static function columnMap($columns, array &$stack, bool $root): array
    {
        if ($columns === '*') {
            return $stack;
        }

        foreach ($columns as $key => $value) {
            if (is_int($key)) {
                preg_match('/([\p{L}_][\p{L}\p{N}@$#\-_]*\.)?(?<column>[\p{L}_][\p{L}\p{N}@$#\-_]*)(?:\s*\((?<alias>[\p{L}_][\p{L}\p{N}@$#\-_]*)\))?(?:\s*\[(?<type>(?:String|Bool|Int|Number|Object|JSON))\])?/u', $value, $keyMatch);

                $columnKey = !empty($keyMatch['alias']) ?
                    $keyMatch['alias'] :
                    $keyMatch['column'];

                $stack[$value] = isset($keyMatch['type']) ?
                    [$columnKey, $keyMatch['type']] :
                    [$columnKey];
            } elseif (static::isRaw($value)) {
                preg_match('/([\p{L}_][\p{L}\p{N}@$#\-_]*\.)?(?<column>[\p{L}_][\p{L}\p{N}@$#\-_]*)(\s*\[(?<type>(String|Bool|Int|Number))\])?/u', $key, $keyMatch);
                $columnKey = $keyMatch['column'];

                $stack[$key] = isset($keyMatch['type']) ?
                    [$columnKey, $keyMatch['type']] :
                    [$columnKey];
            } elseif (!is_int($key) && is_array($value)) {
                if ($root && count(array_keys($columns)) === 1) {
                    $stack[$key] = [$key, 'String'];
                }

                static::columnMap($value, $stack, false);
            }
        }

        return $stack;
    }

    /**
     * Mapping the data from the table.
     *
     * @param array $data
     * @param array $columns
     * @param array $columnMap
     * @param array $stack
     * @param bool $root
     * @param array $result
     * @codeCoverageIgnore
     * @return void
     */
    protected static function dataMap(
        array $data,
        array $columns,
        array $columnMap,
        array &$stack,
        bool $root,
        array &$result = null
    ): void {
        if ($root) {
            $columnsKey = array_keys($columns);

            if (count($columnsKey) === 1 && is_array($columns[$columnsKey[0]])) {
                $indexKey = array_keys($columns)[0];
                $dataKey = preg_replace("/^[\p{L}_][\p{L}\p{N}@$#\-_]*\./u", '', $indexKey);
                $currentStack = [];

                foreach ($data as $item) {
                    static::dataMap($data, $columns[$indexKey], $columnMap, $currentStack, false, $result);
                    $index = $data[$dataKey];

                    if (isset($result)) {
                        $result[$index] = $currentStack;
                    } else {
                        $stack[$index] = $currentStack;
                    }
                }
            } else {
                $currentStack = [];
                static::dataMap($data, $columns, $columnMap, $currentStack, false, $result);

                if (isset($result)) {
                    $result[] = $currentStack;
                } else {
                    $stack = $currentStack;
                }
            }

            return;
        }

        foreach ($columns as $key => $value) {
            $isRaw = static::isRaw($value);

            if (is_int($key) || $isRaw) {
                $map = $columnMap[$isRaw ? $key : $value];
                $columnKey = $map[0];
                $item = $data[$columnKey];

                if (isset($map[1])) {
                    if ($isRaw && in_array($map[1], ['Object', 'JSON'])) {
                        continue;
                    }

                    if (is_null($item)) {
                        $stack[$columnKey] = null;
                        continue;
                    }

                    switch ($map[1]) {

                        case 'Number':
                            $stack[$columnKey] = (float) $item;
                            break;

                        case 'Int':
                            $stack[$columnKey] = (int) $item;
                            break;

                        case 'Bool':
                            $stack[$columnKey] = (bool) $item;
                            break;

                        case 'Object':
                            $stack[$columnKey] = unserialize($item);
                            break;

                        case 'JSON':
                            $stack[$columnKey] = json_decode($item, true);
                            break;

                        case 'String':
                            $stack[$columnKey] = (string) $item;
                            break;
                    }
                } else {
                    $stack[$columnKey] = $item;
                }
            } else {
                $currentStack = [];
                static::dataMap($data, $value, $columnMap, $currentStack, false, $result);

                $stack[$key] = $currentStack;
            }
        }
    }

    /**
     * Build and execute returning query.
     *
     * @param string $query
     * @param array $map
     * @param array $data
     * @return \PDOStatement|null
     */
    private static function returningQuery($query, &$map, &$data): ?PDOStatement
    {
        $returnColumns = array_map(
            function ($value) {
                return $value[0];
            },
            $data
        );

        $cols = [];

        if (!empty($returnColumns)) {
            foreach ($returnColumns as $col) {
                $cols[] = static::columnQuote($col);
            }
        }

        $query .= ' RETURNING ' .
            implode(', ', $cols) .
            ' INTO ' .
            implode(', ', array_keys($data));

        return static::exec($query, $map, function ($statement) use (&$data) {
            // @codeCoverageIgnoreStart
            foreach ($data as $key => $return) {
                if (isset($return[3])) {
                    $statement->bindParam($key, $data[$key][1], $return[2], $return[3]);
                } else {
                    $statement->bindParam($key, $data[$key][1], $return[2]);
                }
            }
            // @codeCoverageIgnoreEnd
        });
    }

    /**
     * Create a table.
     *
     * @param string $table
     * @param array $columns Columns definition.
     * @param array $options Additional table options for creating a table.
     * @return \PDOStatement|null
     */
    public static function create(string $table, $columns, $options = null): ?PDOStatement
    {
        $stack = [];
        $tableOption = '';
        $tableName = static::tableQuote($table);

        foreach ($columns as $name => $definition) {
            if (is_int($name)) {
                $stack[] = preg_replace('/\<([\p{L}_][\p{L}\p{N}@$#\-_]*)\>/u', '"$1"', $definition);
            } elseif (is_array($definition)) {
                $stack[] = static::columnQuote($name) . ' ' . implode(' ', $definition);
            } elseif (is_string($definition)) {
                $stack[] = static::columnQuote($name) . ' ' . $definition;
            }
        }

        if (is_array($options)) {
            $optionStack = [];

            foreach ($options as $key => $value) {
                if (is_string($value) || is_int($value)) {
                    $optionStack[] = "{$key} = {$value}";
                }
            }

            $tableOption = ' ' . implode(', ', $optionStack);
        } elseif (is_string($options)) {
            $tableOption = ' ' . $options;
        }

        $command = 'CREATE TABLE';

        if (in_array(static::$type, ['mysql', 'pgsql', 'sqlite'])) {
            $command .= ' IF NOT EXISTS';
        }

        return static::exec("{$command} {$tableName} (" . implode(', ', $stack) . "){$tableOption}");
    }

    /**
     * Drop a table.
     *
     * @param string $table
     * @return \PDOStatement|null
     */
    public static function drop(string $table): ?PDOStatement
    {
        return static::exec('DROP TABLE IF EXISTS ' . static::tableQuote($table));
    }

    /**
     * Select data from the table.
     *
     * @param string $table
     * @param array $join
     * @param array|string $columns
     * @param array $where
     * @return array|null
     */
    public static function select(string $table, $join, $columns = null, $where = null): ?array
    {
        $map = [];
        $result = [];
        $columnMap = [];

        $args = func_get_args();
        $lastArgs = $args[array_key_last($args)];
        $callback = is_callable($lastArgs) ? $lastArgs : null;

        $where = is_callable($where) ? null : $where;
        $columns = is_callable($columns) ? null : $columns;

        $column = $where === null ? $join : $columns;
        $isSingle = (is_string($column) && $column !== '*');

        $statement = static::exec(static::selectContext($table, $map, $join, $columns, $where), $map);

        static::columnMap($columns, $columnMap, true);

        if (!static::$statement) {
            return $result;
        }

        // @codeCoverageIgnoreStart
        if ($columns === '*') {
            if (isset($callback)) {
                while ($data = $statement->fetch(PDO::FETCH_ASSOC)) {
                    $callback($data);
                }

                return null;
            }

            return $statement->fetchAll(PDO::FETCH_ASSOC);
        }

        while ($data = $statement->fetch(PDO::FETCH_ASSOC)) {
            $currentStack = [];

            if (isset($callback)) {
                static::dataMap($data, $columns, $columnMap, $currentStack, true);

                $callback(
                    $isSingle ?
                        $currentStack[$columnMap[$column][0]] :
                        $currentStack
                );
            } else {
                static::dataMap($data, $columns, $columnMap, $currentStack, true, $result);
            }
        }

        if (isset($callback)) {
            return null;
        }

        if ($isSingle) {
            $singleResult = [];
            $resultKey = $columnMap[$column][0];

            foreach ($result as $item) {
                $singleResult[] = $item[$resultKey];
            }

            return $singleResult;
        }

        return $result;
    }
    // @codeCoverageIgnoreEnd

    /**
     * Insert one or more records into the table.
     *
     * @param string $table
     * @param array $values
     * @param string $primaryKey
     * @return \PDOStatement|null
     */
    public static function insert(string $table, array $values, string $primaryKey = null): ?PDOStatement
    {
        $stack = [];
        $columns = [];
        $fields = [];
        $map = [];
        $returnings = [];

        if (!isset($values[0])) {
            $values = [$values];
        }

        foreach ($values as $data) {
            foreach ($data as $key => $value) {
                $columns[] = $key;
            }
        }

        $columns = array_unique($columns);

        foreach ($values as $data) {
            $values = [];

            foreach ($columns as $key) {
                $value = $data[$key];
                $type = gettype($value);

                if (static::$type === 'oracle' && $type === 'resource') {
                    $values[] = 'EMPTY_BLOB()';
                    $returnings[static::mapKey()] = [$key, $value, PDO::PARAM_LOB];
                    continue;
                }

                if ($raw = static::buildRaw($data[$key], $map)) {
                    $values[] = $raw;
                    continue;
                }

                $mapKey = static::mapKey();
                $values[] = $mapKey;

                switch ($type) {

                    case 'array':
                        $map[$mapKey] = [
                            strpos($key, '[JSON]') === strlen($key) - 6 ?
                                json_encode($value) :
                                serialize($value),
                            PDO::PARAM_STR
                        ];
                        break;

                    case 'object':
                        $value = serialize($value);
                        break;

                    case 'NULL':
                    case 'resource':
                    case 'boolean':
                    case 'integer':
                    case 'double':
                    case 'string':
                        $map[$mapKey] = static::typeMap($value, $type);
                        break;
                }
            }

            $stack[] = '(' . implode(', ', $values) . ')';
        }

        foreach ($columns as $key) {
            $fields[] = static::columnQuote(preg_replace("/(\s*\[JSON\]$)/i", '', $key));
        }

        $query = 'INSERT INTO ' . static::tableQuote($table) . ' (' . implode(', ', $fields) . ') VALUES ' . implode(', ', $stack);

        if (
            static::$type === 'oracle' && (!empty($returnings) || isset($primaryKey))
        ) {
            if ($primaryKey) {
                $returnings[':RETURNID'] = [$primaryKey, '', PDO::PARAM_INT, 8];
            }

            $statement = static::returningQuery($query, $map, $returnings);

            if ($primaryKey) {
                static::$returnId = $returnings[':RETURNID'][1];
            }

            return $statement;
        }

        return static::exec($query, $map);
    }

    /**
     * Modify data from the table.
     *
     * @param string $table
     * @param array $data
     * @param array $where
     * @return \PDOStatement|null
     */
    public static function update(string $table, $data, $where = null): ?PDOStatement
    {
        $fields = [];
        $map = [];
        $returnings = [];

        foreach ($data as $key => $value) {
            $column = static::columnQuote(preg_replace("/(\s*\[(JSON|\+|\-|\*|\/)\]$)/", '', $key));
            $type = gettype($value);

            if (static::$type === 'oracle' && $type === 'resource') {
                $fields[] = "{$column} = EMPTY_BLOB()";
                $returnings[static::mapKey()] = [$key, $value, PDO::PARAM_LOB];
                continue;
            }

            if ($raw = static::buildRaw($value, $map)) {
                $fields[] = "{$column} = {$raw}";
                continue;
            }

            preg_match('/(?<column>[\p{L}_][\p{L}\p{N}@$#\-_]*)(\[(?<operator>\+|\-|\*|\/)\])?/u', $key, $match);

            if (isset($match['operator'])) {
                if (is_numeric($value)) {
                    $fields[] = "{$column} = {$column} {$match['operator']} {$value}";
                }
            } else {
                $mapKey = static::mapKey();
                $fields[] = "{$column} = {$mapKey}";

                switch ($type) {

                    case 'array':
                        $map[$mapKey] = [
                            strpos($key, '[JSON]') === strlen($key) - 6 ?
                                json_encode($value) :
                                serialize($value),
                            PDO::PARAM_STR
                        ];
                        break;

                    case 'object':
                        $value = serialize($value);

                        break;
                    case 'NULL':
                    case 'resource':
                    case 'boolean':
                    case 'integer':
                    case 'double':
                    case 'string':
                        $map[$mapKey] = static::typeMap($value, $type);
                        break;
                }
            }
        }

        $query = 'UPDATE ' . static::tableQuote($table) . ' SET ' . implode(', ', $fields) . static::whereClause($where, $map);

        if (static::$type === 'oracle' && !empty($returnings)) {
            return static::returningQuery($query, $map, $returnings);
        }

        return static::exec($query, $map);
    }

    /**
     * Delete data from the table.
     *
     * @param string $table
     * @param array|Raw $where
     * @return \PDOStatement|null
     */
    public static function delete(string $table, $where): ?PDOStatement
    {
        $map = [];

        return static::exec('DELETE FROM ' . static::tableQuote($table) . static::whereClause($where, $map), $map);
    }

    /**
     * Replace old data with a new one.
     *
     * @param string $table
     * @param array $columns
     * @param array $where
     * @return \PDOStatement|null
     */
    public static function replace(string $table, array $columns, $where = null): ?PDOStatement
    {
        $map = [];
        $stack = [];

        foreach ($columns as $column => $replacements) {
            if (is_array($replacements)) {
                foreach ($replacements as $old => $new) {
                    $mapKey = static::mapKey();
                    $columnName = static::columnQuote($column);
                    $stack[] = "{$columnName} = REPLACE({$columnName}, {$mapKey}a, {$mapKey}b)";

                    $map[$mapKey . 'a'] = [$old, PDO::PARAM_STR];
                    $map[$mapKey . 'b'] = [$new, PDO::PARAM_STR];
                }
            }
        }

        if (empty($stack)) {
            throw new InvalidArgumentException('Invalid columns supplied.');
        }

        return static::exec('UPDATE ' . static::tableQuote($table) . ' SET ' . implode(', ', $stack) . static::whereClause($where, $map), $map);
    }

    /**
     * Get only one record from the table.
     *
     * @param string $table
     * @param array $join
     * @param array|string $columns
     * @param array $where
     * @return mixed
     */
    public static function get(string $table, $join = null, $columns = null, $where = null)
    {
        $map = [];
        $result = [];
        $columnMap = [];
        $currentStack = [];

        if ($where === null) {
            if (static::isJoin($join)) {
                $where['LIMIT'] = 1;
            } else {
                $columns['LIMIT'] = 1;
            }

            $column = $join;
        } else {
            $column = $columns;
            $where['LIMIT'] = 1;
        }

        $isSingle = (is_string($column) && $column !== '*');
        $query = static::exec(static::selectContext($table, $map, $join, $columns, $where), $map);

        if (!static::$statement) {
            return false;
        }

        // @codeCoverageIgnoreStart
        $data = $query->fetchAll(PDO::FETCH_ASSOC);

        if (isset($data[0])) {
            if ($column === '*') {
                return $data[0];
            }

            static::columnMap($columns, $columnMap, true);
            static::dataMap($data[0], $columns, $columnMap, $currentStack, true, $result);

            if ($isSingle) {
                return $result[0][$columnMap[$column][0]];
            }

            return $result[0];
        }
    }
    // @codeCoverageIgnoreEnd

    /**
     * Determine whether the target data existed from the table.
     *
     * @param string $table
     * @param array $join
     * @param array $where
     * @return bool
     */
    public static function has(string $table, $join, $where = null): bool
    {
        $map = [];
        $column = null;

        $query = static::exec(
            static::$type === 'mssql' ?
                static::selectContext($table, $map, $join, $column, $where, Db::raw('TOP 1 1')) :
                'SELECT EXISTS(' . static::selectContext($table, $map, $join, $column, $where, 1) . ')',
            $map
        );

        if (!static::$statement) {
            return false;
        }

        // @codeCoverageIgnoreStart
        $result = $query->fetchColumn();

        return $result === '1' || $result === 1 || $result === true;
    }
    // @codeCoverageIgnoreEnd

    /**
     * Randomly fetch data from the table.
     *
     * @param string $table
     * @param array $join
     * @param array|string $columns
     * @param array $where
     * @return array
     */
    public static function rand(string $table, $join = null, $columns = null, $where = null): array
    {
        $orderRaw = static::raw(
            static::$type === 'mysql' ? 'RAND()'
                : (static::$type === 'mssql' ? 'NEWID()'
                    : 'RANDOM()')
        );

        if ($where === null) {
            if (static::isJoin($join)) {
                $where['ORDER'] = $orderRaw;
            } else {
                $columns['ORDER'] = $orderRaw;
            }
        } else {
            $where['ORDER'] = $orderRaw;
        }

        return static::select($table, $join, $columns, $where);
    }

    /**
     * Build for the aggregate function.
     *
     * @param string $type
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return string|null
     */
    private static function aggregate(string $type, string $table, $join = null, $column = null, $where = null): ?string
    {
        $map = [];

        $query = static::exec(static::selectContext($table, $map, $join, $column, $where, $type), $map);

        if (!static::$statement) {
            return null;
        }

        // @codeCoverageIgnoreStart
        return (string) $query->fetchColumn();
    }
    // @codeCoverageIgnoreEnd

    /**
     * Count the number of rows from the table.
     *
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return int|null
     */
    public static function count(string $table, $join = null, $column = null, $where = null): ?int
    {
        return (int) static::aggregate('COUNT', $table, $join, $column, $where);
    }

    /**
     * Calculate the average value of the column.
     *
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return string|null
     */
    public static function avg(string $table, $join, $column = null, $where = null): ?string
    {
        return static::aggregate('AVG', $table, $join, $column, $where);
    }

    /**
     * Get the maximum value of the column.
     *
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return string|null
     */
    public static function max(string $table, $join, $column = null, $where = null): ?string
    {
        return static::aggregate('MAX', $table, $join, $column, $where);
    }

    /**
     * Get the minimum value of the column.
     *
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return string|null
     */
    public static function min(string $table, $join, $column = null, $where = null): ?string
    {
        return static::aggregate('MIN', $table, $join, $column, $where);
    }

    /**
     * Calculate the total value of the column.
     *
     * @param string $table
     * @param array $join
     * @param string $column
     * @param array $where
     * @return string|null
     */
    public static function sum(string $table, $join, $column = null, $where = null): ?string
    {
        return static::aggregate('SUM', $table, $join, $column, $where);
    }

    /**
     * Start a transaction.
     *
     * @param callable $actions
     * @codeCoverageIgnore
     * @return void
     */
    public static function action(callable $actions): void
    {
        if (is_callable($actions)) {
            static::$pdo->beginTransaction();

            try {
                $result = $actions(new static);

                if ($result === false) {
                    static::$pdo->rollBack();
                } else {
                    static::$pdo->commit();
                }
            } catch (Exception $e) {
                static::$pdo->rollBack();
                throw $e;
            }
        }
    }

    /**
     * Return the ID for the last inserted row.
     *
     * @param string $name
     * @codeCoverageIgnore
     * @return string|null
     */
    public static function id(string $name = null): ?string
    {
        $type = static::$type;

        if ($type === 'oracle') {
            return static::$returnId;
        } elseif ($type === 'pgsql') {
            $id = static::$pdo->query('SELECT LASTVAL()')->fetchColumn();

            return (string) $id ?: null;
        }

        return static::$pdo->lastInsertId($name);
    }

    /**
     * Enable debug mode and output readable statement string.
     *
     * @codeCoverageIgnore
     * @return Db
     */
    public static function debug(): static
    {
        static::$debugMode = true;

        return new static;
    }

    /**
     * Enable debug logging mode.
     *
     * @codeCoverageIgnore
     * @return void
     */
    public static function beginDebug(): void
    {
        static::$debugMode = true;
        static::$debugLogging = true;
    }

    /**
     * Disable debug logging and return all readable statements.
     *
     * @codeCoverageIgnore
     * @return void
     */
    public static function debugLog(): array
    {
        static::$debugMode = false;
        static::$debugLogging = false;

        return static::$debugLogs;
    }

    /**
     * Return the last performed statement.
     *
     * @codeCoverageIgnore
     * @return string|null
     */
    public static function last(): ?string
    {
        if (empty(static::$logs)) {
            return null;
        }

        $log = static::$logs[array_key_last(static::$logs)];

        return static::generate($log[0], $log[1]);
    }

    /**
     * Return all executed statements.
     *
     * @codeCoverageIgnore
     * @return string[]
     */
    public static function log(): array
    {
        return array_map(
            function ($log) {
                return static::generate($log[0], $log[1]);
            },
            static::$logs
        );
    }

    /**
     * Get information about the database connection.
     *
     * @codeCoverageIgnore
     * @return array
     */
    public static function info(): array
    {
        $output = [
            'server' => 'SERVER_INFO',
            'driver' => 'DRIVER_NAME',
            'client' => 'CLIENT_VERSION',
            'version' => 'SERVER_VERSION',
            'connection' => 'CONNECTION_STATUS'
        ];

        foreach ($output as $key => $value) {
            try {
                $output[$key] = static::$pdo->getAttribute(constant('PDO::ATTR_' . $value));
            } catch (PDOException $e) {
                $output[$key] = $e->getMessage();
            }
        }

        $output['dsn'] = static::$dsn;

        return $output;
    }
}
