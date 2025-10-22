<?php

require_once __DIR__ . '/vendor/autoload.php';

use Utopia\Cache\Adapter\None as CacheNone;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;

// Create SQLite database
$dbAdapter = new SQLite(new PDO('sqlite::memory:'));
$dbAdapter->setNamespace('test');
$cache = new Cache(new CacheNone());
$database = new Database($dbAdapter, $cache);

// Create database (metadata tables)
$database->create();

// Create collection
$collectionId = 'test_empty_strings';
$database->createCollection($collectionId);
$database->createAttribute($collectionId, 'text', Database::VAR_STRING, 255, false);

// Create document with empty string
$doc = $database->createDocument($collectionId, new Document([
    '$id' => 'empty_str_doc',
    '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
    'text' => ''
]));

echo "Created document:\n";
echo "text = '" . $doc->getAttribute('text') . "'\n";
echo "text empty? " . (empty($doc->getAttribute('text')) ? 'YES' : 'NO') . "\n";
echo "text === ''? " . ($doc->getAttribute('text') === '' ? 'YES' : 'NO') . "\n\n";

// Try to concatenate
echo "Attempting to concatenate 'hello' to empty string...\n";
$operator = Operator::concat('hello');
echo "Operator created: " . get_class($operator) . "\n";
echo "Is Operator? " . (Operator::isOperator($operator) ? 'YES' : 'NO') . "\n\n";

$updateDoc = new Document(['text' => $operator]);
echo "Update document created\n";
echo "Update doc text attribute: " . get_class($updateDoc->getAttribute('text')) . "\n";
echo "Is Operator? " . (Operator::isOperator($updateDoc->getAttribute('text')) ? 'YES' : 'NO') . "\n\n";

// Perform update
$updated = $database->updateDocument($collectionId, 'empty_str_doc', $updateDoc);

echo "Updated document:\n";
echo "text = '" . $updated->getAttribute('text') . "'\n";
echo "Expected: 'hello'\n";
echo "Match? " . ($updated->getAttribute('text') === 'hello' ? 'YES' : 'NO') . "\n";
