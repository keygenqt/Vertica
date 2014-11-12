<?php

namespace yii\vertica;

use Yii;
use yii\base\Component;
use yii\base\InvalidConfigException;

/**
 * @author Vitaliy Zarubint <keygenqt@gmail.com>
 * @since 2.0
 */
class Connection extends Component
{
    /**
     * @event Event an event that is triggered after a DB connection is established
     */
    const EVENT_AFTER_OPEN = 'afterOpen';

    /**
     * @var array the active connect.
     */
    public $activeConnect;
    
    /**
     * @var string database name
     */
    public static $db;

    /**
     * @var string dsn connect data
     */
    public $dsn;
    
    /**
     * @var string user database vertica
     */
    public $username;
    
    /**
     * @var string password database vertica
     */
    public $password;
    
    private $_table;
    private $_resource;
    
    /**
     * @return string database name
     */
    public function getDb()
    {
        if (self::$db === null && $this->activeConnect) {
            self::$db = $this->exec('SELECT database_name FROM databases')->scalar();
        }
        return self::$db;
    }
    
    /**
     * @param string $table set name table query
     */
    public function setTable($table)
    {
        $this->_table = $table;
    }
    
    /**
     * @return string table name
     */
    public function getTable()
    {
        return $this->_table;
    }
    
    /**
     * @param type $sql
     * @return \yii\vertica\Connection
     */
    public function exec($sql)
    {
        $this->open();
        $this->_resource = odbc_exec($this->activeConnect, $sql);
        return $this;
    }
    
    /**
     * @param string $sql query execute
     * @param array $params not worked
     * @return boolean
     */
    public function execute($sql, $params = [])
    {
        $stmt = odbc_prepare($this->activeConnect, $sql);
        odbc_execute($stmt, $params);
        return true;
    }

    /**
     * Get result query in resource
     * @return resource
     */
    public function resource()
    {
        return $this->_resource;
    }
    
    /**
     * @return array
     */
    public function one()
    {
        return odbc_fetch_array($this->_resource);
    }
    
    /**
     * @return mixed
     */
    public function scalar()
    {
        if ($value = odbc_fetch_array($this->_resource)) {
            return array_shift($value);
        }
        return null;
    }
    
    /**
     * @return array
     */
    public function all()
    {
        $result = [];
        while ($row = odbc_fetch_array($this->_resource)) {
            $result[] = $row;
        }
        return $result;
    }

    /**
     * Returns a value indicating whether the DB connection is established.
     * @return boolean whether the DB connection is established
     */
    public function getIsActive()
    {
        $this->open();
        return $this->activeConnect !== null;
    }

    /**
     * Establishes a DB connection.
     * It does nothing if a DB connection has already been established.
     * @throws Exception if connection fails
     */
    public function open()
    {
        if ($this->activeConnect !== null) {
            return;
        }
        try {   
            $this->activeConnect = odbc_connect($this->dsn, $this->username, $this->password);
        } catch (yii\base\ErrorException $ex) {
            throw new InvalidConfigException($ex->getMessage());
        }
        Yii::trace('Opening connection to vertica.', __CLASS__);
        $this->initConnection();
    }
    
    /**
     * Return is connect to params
     * @param type $dsn
     * @param type $username
     * @param type $password
     * @param type $error
     * @return boolean
     */
    public function isConnect($dsn, $username, $password, $error = false)
    {
        try {
            odbc_connect($dsn, $username, $password);
        } catch (\yii\base\ErrorException $ex) {
            if ($error) {
                return $ex->getMessage();
            }
            return false;
        }
        return !$error;
    }

    /**
     * Closes the currently active DB connection.
     */
    public function close()
    {
        Yii::trace('Closing connection to vertica.', __CLASS__);
        odbc_close($this->activeConnect);
        $this->activeConnect = null;
    }

    /**
     * Initializes the DB connection.
     * This method is invoked right after the DB connection is established.
     * The default implementation triggers an [[EVENT_AFTER_OPEN]] event.
     */
    protected function initConnection()
    {
        $this->trigger(self::EVENT_AFTER_OPEN);
    }

    /**
     * Returns the name of the DB driver for the current [[dsn]].
     * @return string name of the DB driver
     */
    public function getDriverName()
    {
        return 'vertica';
    }

    /**
     * Creates a command for execution.
     * @param array $config the configuration for the Command class
     * @return Command the DB command
     */
    public function createCommand($config = [])
    {
        if (empty($config['db'])) {
            $this->open();
            $config['db'] = $this;
        }
        $command = new Command($config);

        return $command;
    }

    /**
     * Creates new query builder instance
     * @return QueryBuilder
     */
    public function getQueryBuilder()
    {
        return new QueryBuilder($this);
    }
    
    public function quoteColumnName($name)
    {
        return $name;
    }
    
    public function quoteTableName($name)
    {
        return $name;
    }
}
