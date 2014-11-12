<?php

namespace yii\vertica;

use Yii;
use yii\db\BaseActiveRecord;
use yii\helpers\ArrayHelper;

class ActiveRecord extends BaseActiveRecord
{
    public static $primary = [];

    /**
     * @return \yii\vertica\Connection
     */
    public static function getDb()
    {
        $db = \Yii::$app->get('vertica');
        $table = static::tableName();
        $db->setTable($table);
        return $db;
    }

    /**
     * @return \yii\verticaActiveQuery
     */
    public static function find()
    {
        $called = get_called_class();
        $query = Yii::createObject(ActiveQuery::className(), [$called]);
        $query->from = $called::tableName();
        return $query;
    }

    /**
     * @param array $condition
     * @return model
     */
    public static function findOne($condition)
    {
        $query = static::find();
        if (is_array($condition)) {
            return $query->andWhere($condition)->one();
        } else {
            return static::get($condition);
        }
    }
    
    /**
     * @param array $condition
     * @return array models
     */
    public static function findAll($condition)
    {
        $query = static::find();
        if (ArrayHelper::isAssociative($condition)) {
            return $query->andWhere($condition)->all();
        } else {
            return static::mget((array) $condition);
        }
    }

    public function getPrimaryKey($asArray = false)
    {
        $pk = static::primaryKey()[0];
        if ($asArray) {
            return [$pk => $this->$pk];
        } else {
            return $this->$pk;
        }
    }

    public static function primaryKey()
    {
        $class = get_called_class();
        if (!isset(self::$primary[$class])) {
            self::$primary[$class] = self::getDb()->createCommand()->getPk();
        }
        return [self::$primary[$class]];
    }

    public function attributes()
    {
        return self::getDb()->createCommand()->getColumn();
    }

    public function arrayAttributes()
    {
        return self::getDb()->createCommand()->getColumn();
    }
    
    public function safeAttributes()
    {
        return self::getDb()->createCommand()->getColumn();
    }

    public static function populateRecord($record, $row)
    {
        $attributes = [];
        if (!empty($row)) {
            $attributes = $row;
        }
        if (!empty($row)) {
            // reset fields in case it is scalar value
            $arrayAttributes = $record->arrayAttributes();
            foreach($row as $key => $value) {
                if (!isset($arrayAttributes[$key])) {
                    $row[$key] = $value;
                }
            }
            $attributes = array_merge($attributes, $row);
        }
        parent::populateRecord($record, $attributes);
    }

    public function insert($runValidation = true, $attributes = null, $options = ['op_type' => 'create'])
    {
        if ($runValidation && !$this->validate($attributes)) {
            return false;
        }
        if (!$this->beforeSave(true)) {
            return false;
        }
        
        $values = $this->getDirtyAttributes($attributes);
        
        static::getDb()->createCommand()->insert(
            static::tableName(),
            $values
        )->execute();

        $changedAttributes = array_fill_keys(array_keys($values), null);
        $this->setOldAttributes($values);
        $this->afterSave(true, $changedAttributes);

        return true;
    }

    public static function updateAll($attributes, $condition = [])
    {
        $pkName = static::primaryKey()[0];
        if (empty($condition[$pkName])) {
            return 0;
        }
        return self::getDb()->createCommand()->update(static::tableName(), $pkName, $condition[$pkName], $attributes);
    }

    public static function deleteAll($condition = [])
    {
        static::find()->where($condition)->createCommand()->delete()->execute();
        return true;
    }
    
    /**
     * Creates an active record instance.
     *
     * This method is called together with [[populateRecord()]] by [[ActiveQuery]].
     * It is not meant to be used for creating new records directly.
     *
     * You may override this method if the instance being created
     * depends on the row data to be populated into the record.
     * For example, by creating a record based on the value of a column,
     * you may implement the so-called single-table inheritance mapping.
     * @param array $row row data to be populated into the record.
     * This array consists of the following keys:
     *  - `_source`: refers to the attributes of the record.
     *  - `_type`: the type this record is stored in.
     *  - `_index`: the index this record is stored in.
     * @return static the newly created active record
     */
    public static function instantiate($row)
    {
        return new static;
    }
}
