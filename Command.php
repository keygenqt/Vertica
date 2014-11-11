<?php

namespace yii\vertica;

use yii\base\Component;

/**
 * @author Vitaliy Zarubint <keygenqt@gmail.com>
 * @since 2.0
 */
class Command extends Component
{
    /**
     * @var Connection
     */
    public $db;
    public $condition;
    
    public static $attributes;
    public static $pk;
    
    private $_sql;

    public function __construct($config = []) 
    {
        if (isset($config['sql'])) {
            $this->_sql = $config['sql'];
            unset($config['sql']);
        }
        return parent::__construct($config);
    }

    public function search($q = '*')
    {
        return $this->db->exec("SELECT $q FROM {$this->db->table} $this->condition")->all();
    }
    
    public function getColumn()
    {
        if (!isset(self::$attributes[$this->db->table])) {
            $resource = $this->db->exec('SELECT column_name FROM COLUMNS WHERE table_name=' . QueryBuilder::preparationValue($this->db->table))->resource();
            self::$attributes[$this->db->table] = [];
            while ($row = odbc_fetch_array($resource)) {
                self::$attributes[$this->db->table][] = $row['column_name'];
            }
        }
        return self::$attributes[$this->db->table];
    }
    
    public function getColumnData()
    {
        return $this->db->exec('SELECT * FROM COLUMNS WHERE table_name=' . QueryBuilder::preparationValue($this->db->table))->all();
    }
    
    public function getPk()
    {
        if (!isset(self::$pk[$this->db->table])) {
            $resource = $this->db->exec('SELECT is_identity, column_name, data_type FROM COLUMNS WHERE table_name=' . QueryBuilder::preparationValue($this->db->table))->resource();
            self::$pk[$this->db->table] = '';
            while ($row = odbc_fetch_array($resource)) {
                if (empty(self::$pk[$this->db->table]) && $row['data_type'] == 'int') {
                    self::$pk[$this->db->table] = $row['column_name'];
                }
                if ($row['is_identity']) {
                    self::$pk[$this->db->table] = $row['column_name'];
                    break;
                }
            }
        }
        return self::$pk[$this->db->table];
    }
    
    public function getTables()
    {
        return $this->db->exec('SELECT * FROM tables')->all();
    }

    public function insert($table, $columns)
    {
        $this->_sql = "INSERT INTO $table (" . implode(', ', array_keys($columns)) . ")";
        $values = [];
        foreach ($columns as $key => $value) {
            $values[] = QueryBuilder::preparationValue($value);
        }
        $this->_sql .= ' VALUES (' . implode(', ', $values) . ')';
        return $this;
    }

    public function delete($table, $condition = '')
    {
        if (is_array($condition)) {
            $params = [];
            foreach ($condition as $key => $value) {
                $params[] = $key . '=' . QueryBuilder::preparationValue($value);
            }
            $this->_sql = "DELETE FROM $table WHERE " . implode(' AND ', $params);
        } else {
            $this->_sql = "DELETE FROM $table WHERE $condition";
        }
        return $this;
    }

    public function createTable($table, $columns = [], $options = null)
    {
        $cols = [];
        foreach ($columns as $name => $type) {
            if (is_string($name)) {
                $cols[] = "\t" . $name . ' ' . $type;
            } else {
                $cols[] = "\t" . $type;
            }
        }
        $sql = "CREATE TABLE " . $table . " (\n" . implode(",\n", $cols) . "\n)";
        
        $this->_sql = $options === null ? $sql : $sql . ' ' . $options;
        
        return $this;
    }
    
    public function dropTable($table)
    {
        $this->_sql = "DROP TABLE $table";
        return $this;
    }
    
    public function queryOne()
    {
        return $this->db->exec($this->_sql)->one();
    }

    public function queryAll()
    {
        return $this->db->exec($this->_sql)->all();
    }
    
    public function queryScalar()
    {
        return $this->db->exec($this->_sql)->one();
    }

    public function execute($params = [])
    {
        if ($this->_sql) {
            return $this->db->execute($this->_sql, $params);
        }
    }

}
