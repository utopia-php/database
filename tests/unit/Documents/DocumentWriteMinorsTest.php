<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;

final class DocumentWriteMinorsTest extends TestCase
{
    private const string COLLECTION = 'minors';

    private const string WRAPPED = 'wrapped';

    public function testCaseOnlyRenameStoresTheNewCasing(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $adapter = new class ($pdo) extends SQLite
        {
            /**
             * @param  Query[]  $queries
             */
            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                return parent::getDocument($collection, \strtolower($id), $queries, $forUpdate);
            }
        };
        $database = $this->database($adapter);
        $database->createDocument(self::COLLECTION, new Document([
            '$id' => 'abc',
            'name' => 'renamed',
        ]));

        $renamed = $database->updateDocument(self::COLLECTION, 'ABC', new Document(['$id' => 'ABC']));

        $this->assertSame('ABC', $renamed->getId());
        $this->assertSame(
            [['_uid' => 'ABC', 'name' => 'renamed']],
            $this->rows($pdo, 'SELECT _uid, name FROM "'.$database->getNamespace().'_'.self::COLLECTION.'"'),
        );
    }

    public function testFindLeavesTheCallersCursorUnchanged(): void
    {
        $database = $this->database(new SQLite(new PDO('sqlite::memory:')));
        foreach (['first', 'second'] as $id) {
            $database->createDocument(self::COLLECTION, new Document([
                '$id' => $id,
                'secret' => $id,
                'seen' => '2026-01-02T03:04:05.678+00:00',
            ]));
        }
        $cursor = $database->getDocument(self::COLLECTION, 'first');
        $before = $cursor->getArrayCopy();

        $page = $database->find(self::COLLECTION, [Query::cursorAfter($cursor), Query::limit(1)]);

        $this->assertSame(['second'], \array_map(static fn (Document $document): string => $document->getId(), $page));
        $this->assertSame($before, $cursor->getArrayCopy());
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function rows(PDO $pdo, string $sql): array
    {
        $statement = $pdo->query($sql);
        $this->assertNotFalse($statement);

        /** @var list<array<string, mixed>> $rows */
        $rows = $statement->fetchAll(PDO::FETCH_ASSOC);

        return $rows;
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()), [
            self::WRAPPED => [
                'encode' => static fn (mixed $value): ?string => $value === null ? null : \json_encode(['value' => $value], JSON_THROW_ON_ERROR),
                'decode' => static function (mixed $value): mixed {
                    if ($value === null) {
                        return null;
                    }

                    $decoded = \is_string($value) ? \json_decode($value, true) : null;
                    if (! \is_array($decoded) || ! \array_key_exists('value', $decoded)) {
                        throw new RuntimeException('Decoded a value that was never encoded: '.\var_export($value, true));
                    }

                    return $decoded['value'];
                },
            ],
        ]);
        $database->addHook(new Permissions());
        $database
            ->setDatabase('write_minors')
            ->setNamespace('write_minors_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::string('name', size: 64, required: false),
                Attribute::integer('counter', required: false),
                Attribute::string('secret', size: 1024, required: false, filters: [self::WRAPPED]),
                Attribute::string('data', size: 1024, required: false, filters: ['json']),
                Attribute::datetime('seen', required: false, filters: ['datetime']),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: true,
        ));

        return $database;
    }
}
