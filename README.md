# Cleup - Database

#### Installation

Install the `cleup/database` library using composer:

```
composer require cleup/database
```

#### Usage

##### Configuration
```php 
use Cleup\Core\Database\Db;

Db::config([
    'type'     => 'mysql', // pgsql, sybase, oracle, mssql, sqlite
    'host'     => 'localhost',
    'database' => 'db_name',
    'username' => 'db_user',
    'password' => '',
    'port'     => 3306,
    'charset'  => 'utf8mb4',
    'prefix'   => '',
]);
```


##### Methods

```php
use Cleup\Core\Database\Db;

# Select
// select("table name", "colums", "where|order|limit")
Db::select("users", "name");           // Single
Db::select("users", ["id", "name"]);   // Using an array
Db::select("users", "*");              // All columns
Db::select("users", "name", [          // Where syntax
    "email" => "mail@example.com",     // WHERE email = "mail@example.com"
    "id[>]" => 1,                      // WHERE id > 1
    "id[<]" => 2,                      // WHERE id < 2
    "id[>=]" => 7,                     // WHERE id >= 7
    "id[!]" => 5,                      // WHERE id != 5
    "age[<>]" => [20, 97],             // WHERE id BETWEEN 20 AND 97
    "age[<>]" => [30, 50],             // WHERE id NOT BETWEEN 30 AND 50
    "name[~]" => "Jimmy",              // WHERE "name" LIKE '%Jimmy%'
    "name[!~]" => "Jimmy",             // WHERE "name" NOT LIKE '%Jimmy%'
    "OR" => [                          // WHERE id IN (1,5,7) OR email IN ('mail@example.com', 'user@example.com')
        "id" => [1, 5, 7],
        "email" => [
            "mail@example.com", 
            "user@example.com"
        ]
    ],
    "AND" => [                         // WHERE id != 3 AND age = 22
        'id[!]' => 3,
        'age' => 22
    ],
    "ORDER" => "id",                   // ORDER BY FIELD(`id`, 12,15),`name`,`date` DESC
    "ORDER" => [                      
        "id" => [12, 15],
        "name",
        "date" => "DESC"
    ],
    "LIMIT" => 50,                     // LIMIT 50
    "LIMIT" => [10, 50]                // LIMIT 50 OFFSET 10
]);
Db::select("users", "name", [          // Raw
    "ORDER" => Db::raw('RAND()'),
    'LIMIT' => Db::raw('AVG(<age>)')
    'date' => Db::raw('NOW()')
]);    

# Insert
Db::insert("articles", [
    "title" => "My life",
    "email" => "<p></p>",
    "active" => 1
]);
$articleId = $database->id();

# Update
Db::update("articles", [
    "active" => 0,
    "views[+]" => 1,                    // Plus one to the current value 
    "views[-]" => 1,                    // Subtract one to the current value
]);

# Delete
Db::delete("articles", ['id' => 3])
Db::delete("articles", [
    "AND" => [
        "type" => "books",
        "views[<]" => 18
    ]
]);

# Replace
Db::replace("articles", [
    "type" => [
        "article" => "new_article",
        "tasks" => "new_task"
    ],
    "column" => [
        "old_value" => "new_value"
    ]
], [
    "id[>]" => 30
]);

# Get
$email = Db::get("users", "email", [     // $email = "mail@example.com"
    "id" => 1
]);

# Has 
$isAccount = Db::has("users", [          // true | false
    "id"=> 2, 
    'verified' => 'Y'
]);

# Rand
$data = Db::rand("users", "*", [
    'id[>]' => 50
]);

# Count
$count = $database->count("users", [     // 100
    "gender" => "female"
]);

# Query
// The query syntax automatically creates a table prefix and analyzes the data to prepare for execution
$data = Db::query("SELECT <name>, <email> FROM <users>")->fetchAll();
$data = Db::query("SELECT * FROM <users> WHERE <name> = :name AND <age> = :age", [ // Prepared statement
    ":name" => "Mister X",
    ":age" => 18
])->fetchAll(); 

# Quote 
$name = Db::quote("Edward");
$string = "My name is " . $name; // $string = "My name is  \'Edward\'"

# Create Table
Db::create("users", [
    "id" => [
        "INT",
        "NOT NULL",
        "AUTO_INCREMENT",
        "PRIMARY KEY"
    ],
    "name" => [
        "VARCHAR(30)",
        "NOT NULL"
    ]
], [
	"ENGINE" => "MyISAM",
	"AUTO_INCREMENT" => 5
]);

# Drop table
Db::drop("users");

# Logging
Db::insert("users", ["name" => "Edward"]);
$users = Db::select("users", "name", ['id' => 21]);
$log = Db::log(); 
/*
$log = array(
    0 => "INSERT INTO "users" ("name") VALUES (\'Edward\')",
    1 => "SELECT "name" FROM "users" WHERE "id" = 21"
);
*/
```

Based on [Medoo](https://github.com/catfan/Medoo) Database Framework


