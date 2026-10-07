<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as MemoryCache;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Cache\QueryCache;
use Utopia\Database\Cache\Scope;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Filter;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;

final class DocumentFromStorageTest extends TestCase
{
    public function testDropsNonStringPermissionsAndDuplicates(): void
    {
        $document = Document::fromStorage([
            Document::ID => 'legacy',
            Document::PERMISSIONS => [
                42,
                Permission::read(Role::any()),
                null,
                1.5,
                true,
                ['read("any")'],
                Permission::read(Role::any()),
                Permission::update(Role::users()),
            ],
        ]);

        $this->assertSame(
            [Permission::read(Role::any()), Permission::update(Role::users())],
            $document->getAttribute(Document::PERMISSIONS),
        );
        $this->assertSame(['any'], $document->getRead());
        $this->assertSame(['users'], $document->getUpdate());
    }

    public function testTheConstructorStillRejectsNonStringPermissions(): void
    {
        $this->expectException(StructureException::class);
        $this->expectExceptionMessage('Every permission must be of type string');

        new Document([Document::PERMISSIONS => [Permission::read(Role::any()), 42]]);
    }

    public function testBuildsNestedDocumentsLikeTheConstructor(): void
    {
        $data = [
            Document::ID => 'parent',
            Document::PERMISSIONS => [Permission::read(Role::any())],
            'author' => [Document::ID => 'author', 'name' => 'Ada'],
            'category' => [Document::COLLECTION => 'categories', 'name' => 'Books'],
            'comments' => [
                [Document::ID => 'first', 'body' => 'one'],
                [Document::COLLECTION => 'comments', 'body' => 'two'],
                ['body' => 'three'],
                'plain',
            ],
            'tags' => ['a', 'b'],
            'settings' => ['theme' => 'dark', 'nested' => ['depth' => 2]],
            'count' => 3,
        ];

        $document = Document::fromStorage($data);

        $this->assertSame(self::describe(new Document($data)), self::describe($document));
        $this->assertInstanceOf(Document::class, $document->getAttribute('author'));
        $this->assertInstanceOf(Document::class, $document->getAttribute('category'));
        $comments = $document->getArray('comments');
        $this->assertInstanceOf(Document::class, $comments[0]);
        $this->assertInstanceOf(Document::class, $comments[1]);
        $this->assertSame(['body' => 'three'], $comments[2]);
        $this->assertSame('plain', $comments[3]);
    }

    public function testDropsNonStringPermissionsOfNestedDocuments(): void
    {
        $document = Document::fromStorage([
            Document::ID => 'parent',
            'author' => [
                Document::ID => 'author',
                Document::PERMISSIONS => [42, Permission::read(Role::any())],
                'publisher' => [Document::ID => 'publisher', Document::PERMISSIONS => [false, Permission::read(Role::users())]],
            ],
            'comments' => [
                [Document::ID => 'first', Document::PERMISSIONS => [null, Permission::delete(Role::any())]],
            ],
        ]);

        $author = $document->getDocument('author');
        $this->assertSame([Permission::read(Role::any())], $author->getPermissions());
        $this->assertSame([Permission::read(Role::users())], $author->getDocument('publisher')->getPermissions());
        $this->assertSame([Permission::delete(Role::any())], $document->getDocuments('comments')[0]->getPermissions());
    }

    /**
     * @return array<string, array{array<string, mixed>, string}>
     */
    public static function malformed(): array
    {
        return [
            'non-string id' => [[Document::ID => 42], Document::ID.' must be of type string'],
            'permissions that are not an array' => [
                [Document::PERMISSIONS => Permission::read(Role::any())],
                Document::PERMISSIONS.' must be of type array',
            ],
        ];
    }

    /**
     * @param  array<string, mixed>  $data
     */
    #[DataProvider('malformed')]
    public function testRejectsWhatTheConstructorRejectsBesidesPermissionEntries(array $data, string $message): void
    {
        $this->expectException(StructureException::class);
        $this->expectExceptionMessage($message);

        Document::fromStorage($data);
    }

    public function testJsonFilterDecodesADocumentShapedValueWithANonStringPermission(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $collection = Collection::create(id: 'users', attributes: [
            Attribute::string(key: 'prefs', size: 1024, filters: [Filter::Json]),
            Attribute::string(key: 'settings', size: 1024, filters: [Filter::Json]),
        ]);

        $decoded = $database->decode($collection, new Document([
            'prefs' => '{"$id":"x","$permissions":["read(\\"any\\")",42]}',
            'settings' => '{"outer":{"$id":"y","$permissions":[7]}}',
        ]));

        $prefs = $decoded->getAttribute('prefs');
        $this->assertInstanceOf(Document::class, $prefs);
        $this->assertSame([Permission::read(Role::any())], $prefs->getPermissions());

        $settings = $decoded->getAttribute('settings');
        $this->assertIsArray($settings);
        $this->assertInstanceOf(Document::class, $settings['outer']);
        $this->assertSame([], $settings['outer']->getPermissions());
    }

    public function testQueryCacheRebuildsCachedDocumentsWithANonStringPermission(): void
    {
        $queryCache = new QueryCache(new Cache(new MemoryCache()));
        $entry = $queryCache->getEntry(new Scope(), 'users', []);
        $this->assertNotNull($entry);
        $stored = new Document();
        $stored->exchangeArray([Document::ID => 'legacy', Document::PERMISSIONS => [Permission::read(Role::any()), 42]]);
        $this->assertTrue($queryCache->set($entry, [$stored], $queryCache->getGeneration($entry)));

        $cached = $queryCache->get($entry);

        $this->assertNotNull($cached);
        $this->assertCount(1, $cached);
        $this->assertSame('legacy', $cached[0]->getId());
        $this->assertSame([Permission::read(Role::any())], $cached[0]->getPermissions());
    }

    /**
     * A json value 7.x stored with a non-string permission stays readable through a mapped document
     * type, cold and from the cache, and does not block an update of another attribute.
     */
    public function testAMappedTypeReadsAStoredJsonValueWithANonStringPermission(): void
    {
        $reads = 0;
        $adapter = new class ($reads) extends Memory {
            public function __construct(private int &$reads)
            {
                parent::__construct();
            }

            #[\Override]
            public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
            {
                if ($collection->getId() === 'users') {
                    $this->reads++;
                }

                return parent::getDocument($collection, $id, $queries, $forUpdate);
            }
        };
        $database = new Database($adapter, new Cache($this->jsonRoundTripCache()));
        $database
            ->setDatabase('from_storage')
            ->setNamespace('from_storage_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'users',
            attributes: [
                Attribute::string(key: 'name', size: 64),
                Attribute::string(key: 'prefs', size: 1024, filters: [Filter::Json]),
            ],
            permissions: [Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $user = new class ([]) extends Document {
        };
        $database->setDocumentType('users', $user::class);
        $database->getAuthorization()->skip(fn (): Document => $adapter->createDocument($database->getCollection('users'), new Document([
            Document::ID => 'legacy',
            Document::PERMISSIONS => [],
            Document::CREATED_AT => '2024-01-01T00:00:00.000+00:00',
            Document::UPDATED_AT => '2024-01-01T00:00:00.000+00:00',
            'name' => 'Ada',
            'prefs' => '{"$id":"x","$permissions":["read(\\"any\\")",42],"theme":"dark"}',
        ])));

        $cold = $database->getDocument('users', 'legacy');
        $cached = $database->getDocument('users', 'legacy');
        $this->assertSame(1, $reads, 'The second read must be served from the cache');

        foreach (['cold' => $cold, 'cached' => $cached] as $read => $document) {
            $this->assertInstanceOf($user::class, $document, $read);
            $prefs = $document->getAttribute('prefs');
            $this->assertInstanceOf(Document::class, $prefs, $read);
            $this->assertSame('dark', $prefs->getAttribute('theme'), $read);
            $this->assertSame([Permission::read(Role::any())], $prefs->getPermissions(), $read);
        }

        $found = $database->find('users');
        $this->assertCount(1, $found);
        $this->assertInstanceOf($user::class, $found[0]);

        $renamed = $database->updateDocument('users', 'legacy', new Document(['name' => 'Grace']));
        $this->assertSame('Grace', $renamed->getAttribute('name'));
    }

    public function testAStorageRebuildKeepsTheMappedTypeAndDropsNonStringPermissions(): void
    {
        $user = new class ([]) extends Document {
        };
        $database = new class (new Memory(), new Cache(new None())) extends Database {
            /**
             * @param  array<string, mixed>  $data
             */
            public function rebuild(string $collection, array $data): Document
            {
                return $this->createDocumentInstance($collection, $data);
            }
        };
        $database->setDocumentType('users', $user::class);

        $document = $database->rebuild('users', [
            Document::ID => 'legacy',
            Document::PERMISSIONS => [Permission::read(Role::any()), 42],
            'prefs' => [Document::ID => 'x', Document::PERMISSIONS => [7, Permission::update(Role::any())]],
            'devices' => [[Document::ID => 'phone', Document::PERMISSIONS => [false, Permission::delete(Role::any())]]],
        ]);

        $this->assertInstanceOf($user::class, $document);
        $this->assertSame([Permission::read(Role::any())], $document->getPermissions());
        $this->assertSame([Permission::update(Role::any())], $document->getDocument('prefs')->getPermissions());
        $this->assertSame([Permission::delete(Role::any())], $document->getDocuments('devices')[0]->getPermissions());
    }

    private function jsonRoundTripCache(): MemoryCache
    {
        return new class () extends MemoryCache {
            #[\Override]
            public function save(string $key, array|string $data, string $hash = '', int $ttl = 0): bool|string|array
            {
                if (\is_array($data)) {
                    /** @var array<int|string, mixed> $data */
                    $data = \json_decode((string) \json_encode($data), true);
                }

                return parent::save($key, $data, $hash);
            }
        };
    }

    /**
     * @return array<int|string, mixed>
     */
    private static function describe(Document $document): array
    {
        return \array_map(self::describeValue(...), \iterator_to_array($document));
    }

    private static function describeValue(mixed $value): mixed
    {
        if ($value instanceof Document) {
            return [Document::class => self::describe($value)];
        }

        if (\is_array($value)) {
            return \array_map(self::describeValue(...), $value);
        }

        return $value;
    }
}
