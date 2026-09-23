<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Builder\Statement;

/**
 * Database::from() and execute() read and write a table as it is stored, past every permission,
 * so they only work while authorization is disabled. Bob may create posts but read none of
 * Alice's, and may not read the private collection at all.
 */
final class QueryBuilderAuthorizationTest extends TestCase
{
    private const string POSTS = 'posts';

    private const string PRIVATE = 'private';

    private PDO $pdo;

    private Database $database;

    private Authorization $authorization;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->authorization = new Authorization();
        $this->database = new Database(new SQLite($this->pdo), new Cache(new None()));
        $this->database
            ->setAuthorization($this->authorization)
            ->setDatabase('builder')
            ->setNamespace('authorization');
        $this->database->addHook(new Permissions());
        $this->database->create();

        $this->authorization->skip(function (): void {
            $this->database->createCollection(new Collection(
                id: self::POSTS,
                attributes: [Attribute::string(key: 'title', size: 64, required: true)],
                permissions: [Permission::create(Role::users())],
                documentSecurity: true,
            ));
            $this->database->createCollection(new Collection(
                id: self::PRIVATE,
                attributes: [Attribute::string(key: 'title', size: 64, required: true)],
                permissions: [],
                documentSecurity: false,
            ));
            $this->database->createDocument(self::POSTS, new Document([
                '$id' => 'secret',
                '$permissions' => [Permission::read(Role::user('alice')), Permission::update(Role::user('alice'))],
                'title' => 'alice only',
            ]));
            $this->database->createDocument(self::PRIVATE, new Document(['$id' => 'hidden', 'title' => 'private row']));
        });

        $this->authorization->addRole(Role::any()->toString());
        $this->authorization->addRole(Role::users()->toString());
        $this->authorization->addRole(Role::user('bob')->toString());
    }

    public function testDirectReadsHideWhatBobMayNotRead(): void
    {
        $this->assertSame([], $this->database->find(self::POSTS));
        $this->assertTrue($this->database->getDocument(self::POSTS, 'secret')->isEmpty());

        $this->expectException(AuthorizationException::class);
        $this->database->find(self::PRIVATE);
    }

    public function testFromIsRefusedWhileAuthorizationIsEnabled(): void
    {
        foreach ([self::POSTS, self::PRIVATE] as $collection) {
            try {
                $this->database->from($collection);
                $this->fail("from({$collection}) must not hand Bob a builder that reads past his permissions");
            } catch (AuthorizationException $exception) {
                $this->assertStringContainsString('skip', $exception->getMessage(), 'The refusal must say how to use the builder');
            }
        }
    }

    public function testStatementsBuiltInsideSkipDoNotRunOutsideIt(): void
    {
        $builder = $this->authorization->skip(fn () => $this->database->from(self::POSTS)->select(['$id', 'title']));
        $read = $builder->build();
        $write = $this->authorization->skip(fn (): Statement => $this->database->from(self::POSTS)
            ->set(['title' => 'defaced'])
            ->filter([Query::equal('$id', ['secret'])])
            ->update());

        foreach ([
            'the builder' => fn () => $builder->execute(),
            'a read statement' => fn () => $read->execute(),
            'a read statement through Database::execute()' => fn () => $this->database->execute($read),
            'the builder through Database::execute()' => fn () => $this->database->execute($builder),
            'a write statement' => fn () => $write->execute(),
            'a write statement through Database::execute()' => fn () => $this->database->execute($write),
        ] as $label => $run) {
            try {
                $run();
                $this->fail("Running {$label} with authorization enabled must be refused");
            } catch (AuthorizationException) {
            }
        }

        $this->assertSame('alice only', $this->title('secret'), 'A refused write must not reach the table');
    }

    public function testTheBuilderReadsAndWritesPastPermissionsInsideSkip(): void
    {
        $rows = $this->authorization->skip(fn () => $this->database->from(self::POSTS)->select(['$id', 'title'])->execute());

        $this->assertIsArray($rows);
        $this->assertCount(1, $rows);
        $this->assertInstanceOf(Document::class, $rows[0]);
        $this->assertSame('alice only', $rows[0]->getAttribute('title'));

        $affected = $this->authorization->skip(fn () => $this->database->from(self::POSTS)
            ->set(['title' => 'edited'])
            ->filter([Query::equal('$id', ['secret'])])
            ->update()
            ->execute());

        $this->assertSame(1, $affected);
        $this->assertSame('edited', $this->title('secret'));
        $this->assertTrue($this->authorization->getStatus(), 'skip() must restore authorization afterwards');
    }

    private function title(string $id): string
    {
        $statement = $this->pdo->prepare('SELECT title FROM authorization_'.self::POSTS.' WHERE _uid = ?');
        $statement->execute([$id]);
        $title = $statement->fetchColumn();
        $this->assertIsString($title);

        return $title;
    }
}
