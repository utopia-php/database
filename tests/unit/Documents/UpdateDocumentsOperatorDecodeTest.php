<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Operator;
use Utopia\Database\Query;

final class UpdateDocumentsOperatorDecodeTest extends TestCase
{
    private const string COLLECTION = 'counters';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()), [
            'wrapped' => [
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
        $this->database
            ->setDatabase('operator_decode')
            ->setNamespace('operator_decode_'.\uniqid());
        $this->database->getAuthorization()->addRole(Role::any()->toString());
        $this->database->create();
        $this->database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer('counter', required: false),
                Attribute::string('secret', size: 128, required: false, filters: ['wrapped']),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ));
        $this->database->createDocuments(self::COLLECTION, [
            new Document(['$id' => 'first', 'counter' => 1, 'secret' => 'alpha']),
            new Document(['$id' => 'second', 'counter' => 10, 'secret' => 'beta']),
        ]);
    }

    public function testOperatorUpdateHandsEachDocumentToOnNextDecodedOnce(): void
    {
        $updated = [];

        $modified = $this->database->updateDocuments(
            self::COLLECTION,
            new Document(['counter' => Operator::increment(1)]),
            [Query::orderAsc('counter')],
            onNext: function (Document $document) use (&$updated): void {
                $updated[$document->getId()] = [$document->getAttribute('counter'), $document->getAttribute('secret')];
            },
        );

        $this->assertSame(2, $modified);
        $this->assertSame([
            'first' => [2, 'alpha'],
            'second' => [11, 'beta'],
        ], $updated);
    }

    public function testOperatorUpdateWithSelectionsDecodesTheSelectedAttributesOnce(): void
    {
        $updated = [];

        $this->database->updateDocuments(
            self::COLLECTION,
            new Document(['counter' => Operator::increment(5)]),
            [Query::select(['counter', 'secret']), Query::equal('$id', ['first'])],
            onNext: function (Document $document) use (&$updated): void {
                $updated[] = [$document->getAttribute('counter'), $document->getAttribute('secret')];
            },
        );

        $this->assertSame([[6, 'alpha']], $updated);
    }

    public function testPlainUpdateStillDecodesTheWrittenDocuments(): void
    {
        $updated = [];

        $this->database->updateDocuments(
            self::COLLECTION,
            new Document(['counter' => 7]),
            [Query::orderAsc('counter')],
            onNext: function (Document $document) use (&$updated): void {
                $updated[$document->getId()] = [$document->getAttribute('counter'), $document->getAttribute('secret')];
            },
        );

        $this->assertSame([
            'first' => [7, 'alpha'],
            'second' => [7, 'beta'],
        ], $updated);
    }

    public function testOperatorUpdatePersistsTheEncodedValueUntouched(): void
    {
        $this->database->updateDocuments(self::COLLECTION, new Document(['counter' => Operator::increment(1)]));

        $document = $this->database->getDocument(self::COLLECTION, 'first');

        $this->assertSame(2, $document->getAttribute('counter'));
        $this->assertSame('alpha', $document->getAttribute('secret'));
    }
}
