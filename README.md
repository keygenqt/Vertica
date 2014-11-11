Vertica Query and ActiveRecord for Yii 2
==============================================

(Alpha version - work: migrations, grid (yet without filters), finds, save well, etc.)

This extension provides the [vertica](https://my.vertica.com/vertica-documentation/) integration for the Yii2 framework.
It includes basic querying/search support and also implements the `ActiveRecord` pattern that allows you to store active
records in vertica.

To use this extension, you have to configure the Connection class in your application configuration:

```php
return [
    //....
    'components' => [
        'vertica' => [
            'class' => 'yii\vertica\Connection',
            'dsn' => 'Driver=Vertica;Server=localhost;Database=my-data-base;',
            'username' => 'username',
            'password' => 'password-base',
        ],
    ]
];
```

and console.php for migrate

```php
return [
    'controllerMap' => [
        'migrate-vertica' => 'yii\vertica\controllers\MigrateVerticaController',
    ],
];
```

Requirements
------------

Performance tested on version 7.1.1

Installation
------------

The preferred way to install this extension is through [composer](http://getcomposer.org/download/).

Either run

```
php composer.phar require --prefer-dist yiisoft/yii2-vertica "*"
```

or add

```json
"yiisoft/yii2-vertica": "*"
```

to the require section of your composer.json.

Using the ActiveRecord
----------------------

The following is an example model called `Customer`:

```php
namespace app\models;

use \yii\data\ActiveDataProvider;
use \yii\vertica\ActiveRecord;

class Admins extends ActiveRecord
{
    public static function tableName() 
    {
        return 'admins';
    }

    /**
	 * @return array validation rules for model attributes.
	 */
    public function rules()
    {
        return [
            [['username', 'password_hash', 'blocked_at', 'role', 'created_at', 'updated_at'], 'safe']
        ];
    }

    /**
	 * @return array attributes labels.
	 */
	public function attributeLabels()
    {
        return [];
    }
    
    public function search($params)
    {
        $query = Admins::find();
        $dataProvider = new ActiveDataProvider([
            'query' => $query,
        ]);

        if (!($this->load($params) && $this->validate())) {
            return $dataProvider;
        }

        $query->andFilterWhere(['like', 'username', $this->username]);
        $query->andFilterWhere(['like', 'password_hash', $this->password_hash]);
        $query->andFilterWhere(['=', 'created_at', $this->created_at]);
        $query->andFilterWhere(['=', 'updated_at', $this->updated_at]);
		
        return $dataProvider;
    }
}
```