<?php

// namespace Utopia\Database\Adapter;

// use PDO;
// use Exception;
// use phpDocumentor\Reflection\DocBlock\Tags\Var_;
// use Utopia\Database\Adapter;

// class Postgres extends Adapter
// {
//     /**
//      * @var PDO
//      */
//     protected $pdo;

//     /**
//      * Constructor.
//      *
//      * Set connection and settings
//      *
//      * @param PDO $pdo
//      */
//     public function __construct(PDO $pdo)
//     {
//         $this->pdo = $pdo;
//     }
    
//     /**
//      * Create Database
//      * 
//      * @return bool
//      */
//     public function create(): bool
//     {
//         $name = $this->getNamespace();

//         return $this->getPDO()
//             ->prepare("CREATE SCHEMA {$name} /*!40100 DEFAULT CHARACTER SET utf8mb4 */;")
//             ->execute();
//     }

//     /**
//      * List Databases
//      * 
//      * @return array
//      */
//     public function list(): array
//     {
//         $stmt = $this->getPDO()
//             ->prepare("SELECT datname FROM pg_database;");

//         $stmt->execute();
        
//         $list = [];

//         foreach ($stmt->fetchAll() as $key => $value) {
//             $list[] = $value['datname'] ?? '';
//         }

//         return $list;
//     }

//     /**
//      * Delete Database
//      * 
//      * @return bool
//      */
//     public function delete(): bool
//     {
//         $name = $this->getNamespace();

//         return $this->getPDO()
//             ->prepare("DROP SCHEMA {$name};")
//             ->execute();
//     }

//     /**
//      * Create Collection
//      * 
//      * @param string $id
//      * @return bool
//      */
//     public function createCollection(string $id): bool
//     {
//         $name = $this->filter($id).'_documents';

//         return $this->getPDO()
//             ->prepare("CREATE TABLE {$this->getNamespace()}.{$name}(
//                 _id     INT         PRIMARY KEY     NOT NULL,
//                 _uid    CHAR(13)                    NOT NULL
//              );")
//             ->execute();
//     }

//     /**
//      * List Collections
//      * 
//      * @return array
//      */
//     public function listCollections(): array
//     {
//     }

//     /**
//      * Delete Collection
//      * 
//      * @param string $id
//      * @return bool
//      */
//     public function deleteCollection(string $id): bool
//     {
//         $name = $this->filter($id).'_documents';

//         return $this->getPDO()
//             ->prepare("DROP TABLE {$this->getNamespace()}.{$name};")
//             ->execute();
//     }

//     /**
//      * @return PDO
//      *
//      * @throws Exception
//      */
//     protected function getPDO()
//     {
//         return $this->pdo;
//     }
// }