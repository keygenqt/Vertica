<?php

namespace yii\vertica;

use yii\db\ActiveQueryInterface;
use yii\db\ActiveQueryTrait;
use yii\db\ActiveRelationTrait;

class ActiveQuery extends Query implements ActiveQueryInterface
{
    use ActiveQueryTrait;
    use ActiveRelationTrait;

    /**
     * Constructor.
     * @param array $modelClass the model class associated with this query
     * @param array $config configurations to be applied to the newly created query object
     */
    public function __construct($modelClass, $config = [])
    {
        $this->modelClass = $modelClass;
        parent::__construct($config);
    }

    /**
     * Creates a DB command that can be used to execute this query.
     * @param Connection $db the DB connection used to create the DB command.
     * If null, the DB connection returned by [[modelClass]] will be used.
     * @return Command the created DB command instance.
     */
    public function createCommand($db = null)
    {
        if ($this->primaryModel !== null) {
            // lazy loading
            if (is_array($this->via)) {
                // via relation
                /* @var $viaQuery ActiveQuery */
                list($viaName, $viaQuery) = $this->via;
                if ($viaQuery->multiple) {
                    $viaModels = $viaQuery->all();
                    $this->primaryModel->populateRelation($viaName, $viaModels);
                } else {
                    $model = $viaQuery->one();
                    $this->primaryModel->populateRelation($viaName, $model);
                    $viaModels = $model === null ? [] : [$model];
                }
                $this->filterByModels($viaModels);
            } else {
                $this->filterByModels([$this->primaryModel]);
            }
        }

        /* @var $modelClass ActiveRecord */
        $modelClass = $this->modelClass;
        if ($db === null) {
            $db = $modelClass::getDb();
        }
        
        return $db->createCommand(['sql' => $db->getQueryBuilder()->build($this)]);
    }

    /**
     * Executes query and returns all results as an array.
     * @param Connection $db the DB connection used to create the DB command.
     * If null, the DB connection returned by [[modelClass]] will be used.
     * @return array the query results. If the query results in nothing, an empty array will be returned.
     */
    public function all($db = null)
    {
        if ($this->asArray) {
            // TODO implement with
            return parent::all($db);
        }

        $result = $this->createCommand($db)->search();

        if (empty($result)) {
            return [];
        }
        $models = $this->createModels($result);
        if (!empty($this->with)) {
            $this->findWith($this->with, $models);
        }
        foreach ($models as $model) {
            $model->afterFind();
        }

        return $models;
    }

    /**
     * Executes query and returns a single row of result.
     * @param Connection $db the DB connection used to create the DB command.
     * If null, the DB connection returned by [[modelClass]] will be used.
     * @return ActiveRecord|array|null a single row of query result. Depending on the setting of [[asArray]],
     * the query result may be either an array or an ActiveRecord object. Null will be returned
     * if the query results in nothing.
     */
    public function one($db = null)
    {
        if (($result = parent::one($db)) === false) {
            return null;
        }
        if ($this->asArray) {
            return $result;
        } else {
            /* @var $class ActiveRecord */
            $class = $this->modelClass;
            $model = $class::instantiate($result);
            $class::populateRecord($model, $result);
            if (!empty($this->with)) {
                $models = [$model];
                $this->findWith($this->with, $models);
                $model = $models[0];
            }
            $model->afterFind();
            return $model;
        }
    }

    /**
     * @inheritdoc
     */
    public function search($db = null, $options = [])
    {
        $result = $this->createCommand($db)->search($options);
        // TODO implement with() for asArray
        if (!empty($result['hits']['hits']) && !$this->asArray) {
            $models = $this->createModels($result['hits']['hits']);
            if (!empty($this->with)) {
                $this->findWith($this->with, $models);
            }
            foreach ($models as $model) {
                $model->afterFind();
            }
            $result['hits']['hits'] = $models;
        }

        return $result;
    }

    /**
     * @inheritdoc
     */
    public function column($db = null)
    {
        if ($field == '_id') {
            $command = $this->createCommand($db);
            $command->queryParts['fields'] = [];
            $command->queryParts['_source'] = false;
            $result = $command->search();
            if (empty($result['hits']['hits'])) {
                return [];
            }
            $column = [];
            foreach ($result['hits']['hits'] as $row) {
                $column[] = $row['_id'];
            }

            return $column;
        }

        return parent::column($field, $db);
    }
}
