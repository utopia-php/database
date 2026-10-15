<?php

namespace Tests\Unit\Filter;

use PHPUnit\Framework\TestCase;
use Tests\Unit\FilterRegistry;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Filter\Callback;
use Utopia\Database\Filter\Codec;
use Utopia\Database\Filter\Registry;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Query\Schema\ColumnType;

final class CodecTest extends TestCase
{
    public function testRegisteredTypeEncodesAndDecodesOnItsHandle(): void
    {
        $adapter = new Memory();
        $registry = new Registry();
        $database = $this->database($adapter)->setFilters($registry);
        $registry->register(new Reversed());

        $this->createNote($database, 'hello', ['reversed']);

        $this->assertSame('olleh', $adapter->getDocument($database->getCollection('notes'), 'note')->getAttribute('body'));
        $this->assertSame('hello', $database->getDocument('notes', 'note')->getAttribute('body'));
    }

    public function testHandlesWithoutTheRegistryCannotEncodeItsTypes(): void
    {
        $this->database()->setFilters($this->registry(new Reversed()));

        $this->expectException(NotFoundException::class);
        $this->database()->encode($this->notes(), new Document(['body' => 'hello']));
    }

    public function testHandlesWithoutTheRegistryCannotDecodeItsTypes(): void
    {
        $this->database()->setFilters($this->registry(new Reversed()));

        $this->expectException(NotFoundException::class);
        $this->database()->decode($this->notes(), new Document(['body' => 'olleh']));
    }

    public function testABuiltInNameCannotBreakOtherHandles(): void
    {
        $adapter = new Memory();
        $this->createNote($this->database($adapter), 'hello');

        $registry = new Registry();
        $this->database()->setFilters($registry);

        try {
            $registry->register(new Reversed('json'));
        } catch (DuplicateException) {
        }

        $this->assertSame('hello', $this->database($adapter)->getDocument('notes', 'note')->getAttribute('body'));
    }

    public function testARegisteredTypeShadowsAGlobalFilterOnlyOnItsHandle(): void
    {
        $database = $this->database()->setFilters($this->registry(new Reversed()));
        $other = $this->database();

        $previous = FilterRegistry::filters();

        try {
            Database::addFilter(
                'reversed',
                static fn (mixed $value): mixed => $value,
                static fn (mixed $value): string => 'global',
            );

            $this->assertSame('hello', $database->decode($this->notes(), new Document(['body' => 'olleh']))->getAttribute('body'));
            $this->assertSame('global', $other->decode($this->notes(), new Document(['body' => 'olleh']))->getAttribute('body'));
        } finally {
            FilterRegistry::restore($previous, FilterRegistry::defaultsRegistered());
        }
    }

    public function testRegisteringATypeChangesOnlyItsHandlesCacheKeys(): void
    {
        $plain = $this->database();
        $before = $this->documentHash($plain);

        $typed = $this->database()->setFilters($this->registry(new Reversed()));

        $this->assertNotSame($before, $this->documentHash($typed));
        $this->assertSame($before, $this->documentHash($plain));
    }

    public function testCacheKeysFollowTheTypeClass(): void
    {
        $reversed = $this->database()->setFilters($this->registry(new Reversed('text')));
        $rot13 = $this->database()->setFilters($this->registry(new Rot13('text')));
        $sameClass = $this->database()->setFilters($this->registry(new Reversed('text')));

        $this->assertNotSame($this->documentHash($reversed), $this->documentHash($rot13));
        $this->assertNotSame(
            $reversed->getQueryCacheField(null, [Query::limit(1)]),
            $rot13->getQueryCacheField(null, [Query::limit(1)]),
        );
        $this->assertSame($this->documentHash($reversed), $this->documentHash($sameClass));
    }

    public function testCacheKeysFollowTheSignatureOfASignedCodec(): void
    {
        $first = $this->database()->setFilters($this->registry(new CodecTestPrefixed('a:')));
        $second = $this->database()->setFilters($this->registry(new CodecTestPrefixed('b:')));
        $sameSignature = $this->database()->setFilters($this->registry(new CodecTestPrefixed('a:')));

        $this->assertNotSame($this->documentHash($first), $this->documentHash($second));
        $this->assertSame($this->documentHash($first), $this->documentHash($sameSignature));
    }

    public function testConstructorFiltersEncodeAndDecodeOnTheirHandle(): void
    {
        $adapter = new Memory();
        $database = $this->database($adapter, [new Reversed()]);

        $this->createNote($database, 'hello', ['reversed']);

        $this->assertSame('olleh', $adapter->getDocument($database->getCollection('notes'), 'note')->getAttribute('body'));
        $this->assertSame('hello', $database->getDocument('notes', 'note')->getAttribute('body'));
        $this->assertTrue($database->getFilters()->has('reversed'));
    }

    public function testSetFiltersReplacesTheConstructorFilters(): void
    {
        $identity = static fn (mixed $value): mixed => $value;
        $database = $this->database(filters: [new Callback('reversed', $identity, $identity)])
            ->setFilters($this->registry(new Reversed()));
        $registryOnly = $this->database()->setFilters($this->registry(new Reversed()));

        $this->assertSame('hello', $database->decode($this->notes(), new Document(['body' => 'olleh']))->getAttribute('body'));
        $this->assertSame($this->documentHash($registryOnly), $this->documentHash($database));
    }

    public function testAConstructorFilterNamedAfterABuiltInIsRejected(): void
    {
        $this->expectException(DuplicateException::class);

        $this->database(filters: [new Reversed('json')]);
    }

    /**
     * @param  list<Codec>  $filters
     */
    private function database(?Memory $adapter = null, array $filters = []): Database
    {
        return (new Database($adapter ?? new Memory(), new Cache(new None()), $filters))
            ->setDatabase('types')
            ->setNamespace('types');
    }

    private function registry(Codec $type): Registry
    {
        $registry = new Registry();
        $registry->register($type);

        return $registry;
    }

    private function notes(): Document
    {
        return new Document([
            '$id' => 'notes',
            'attributes' => [
                new Document([
                    '$id' => 'body',
                    'type' => ColumnType::String->value,
                    'array' => false,
                    'filters' => ['reversed'],
                ]),
            ],
        ]);
    }

    /**
     * @param  list<string>  $filters
     */
    private function createNote(Database $database, string $body, array $filters = []): void
    {
        $database->create();
        $database->createCollection(Collection::create(id: 'notes'));
        $database->createAttribute('notes', Attribute::string(key: 'body', filters: $filters));
        $database->createDocument('notes', new Document([
            '$id' => 'note',
            '$permissions' => [Permission::read(Role::any())],
            'body' => $body,
        ]));
    }

    private function documentHash(Database $database): string
    {
        return $database->getCacheKeys('notes', 'note')[2];
    }
}
