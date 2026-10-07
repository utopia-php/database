<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Interceptor;
use Utopia\Database\Hook\WriteContext;
use Utopia\Database\Validator\Authorization;

final class SQLDeleteFailureTest extends TestCase
{
    private const string NAMESPACE = 'delete_failure';

    private PDO $pdo;

    private SQLite $adapter;

    private Database $database;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->adapter = new SQLite($this->pdo);
        $this->database = new Database($this->adapter, new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: 'notes',
            attributes: [Attribute::string('body', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            documentSecurity: false,
        ));
        $this->database->createDocument('notes', new Document(['$id' => 'first', 'body' => 'one']));
        $this->database->createDocument('notes', new Document(['$id' => 'second', 'body' => 'two']));
    }

    public function testAFailingWriteHookFailsTheDeleteAndKeepsTheDocument(): void
    {
        $failure = new RuntimeException('hook');
        $this->adapter->addWriteHook($this->failingHook($failure));

        try {
            $this->database->deleteDocument('notes', 'first');
            $this->fail('A failing write hook must fail the delete');
        } catch (DatabaseException $error) {
            $this->assertSame('hook', $error->getMessage());
            $this->assertSame($failure, $this->rootCause($error));
        }

        $this->assertSame(['first', 'second'], $this->storedIds());
    }

    public function testAFailingWriteHookFailsTheBulkDeleteAndKeepsTheDocuments(): void
    {
        $failure = new RuntimeException('hook');
        $this->adapter->addWriteHook($this->failingHook($failure));

        try {
            $this->database->deleteDocuments('notes');
            $this->fail('A failing write hook must fail the bulk delete');
        } catch (DatabaseException $error) {
            $this->assertSame('hook', $error->getMessage());
            $this->assertSame($failure, $this->rootCause($error));
        }

        $this->assertSame(['first', 'second'], $this->storedIds());
    }

    public function testADeleteTheEngineRefusesInSilentModeIsAnError(): void
    {
        $this->blockDeletes();

        try {
            $this->adapter->deleteDocument('notes', 'first');
            $this->fail('A delete the engine refuses must not be reported as done');
        } catch (DatabaseException $error) {
            $this->assertSame('Failed to delete document', $error->getMessage());
        }

        $this->assertSame(['first', 'second'], $this->storedIds());
    }

    public function testABulkDeleteTheEngineRefusesInSilentModeIsAnError(): void
    {
        $this->blockDeletes();
        $sequences = [];
        foreach ($this->database->find('notes') as $document) {
            $sequence = $document->getSequence();
            $this->assertNotNull($sequence);
            $sequences[] = $sequence;
        }

        try {
            $this->adapter->deleteDocuments('notes', $sequences, ['first', 'second']);
            $this->fail('A bulk delete the engine refuses must not be reported as done');
        } catch (DatabaseException $error) {
            $this->assertSame('Failed to delete documents', $error->getMessage());
        }

        $this->assertSame(['first', 'second'], $this->storedIds());
    }

    private function blockDeletes(): void
    {
        $this->pdo->exec('CREATE TRIGGER block_deletes BEFORE DELETE ON `' . self::NAMESPACE . "_notes` BEGIN SELECT RAISE(ABORT, 'deletes are blocked'); END");
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_SILENT);
    }

    private function failingHook(RuntimeException $failure): Interceptor
    {
        return new class ($failure) extends Interceptor {
            public function __construct(private readonly RuntimeException $failure)
            {
            }

            public function afterDocumentDelete(string $collection, array $documentIds, WriteContext $context): void
            {
                throw $this->failure;
            }
        };
    }

    private function rootCause(\Throwable $error): \Throwable
    {
        while ($error->getPrevious() !== null) {
            $error = $error->getPrevious();
        }

        return $error;
    }

    /**
     * @return list<string>
     */
    private function storedIds(): array
    {
        $statement = $this->pdo->query('SELECT _uid FROM `' . self::NAMESPACE . '_notes` ORDER BY _uid');
        $this->assertInstanceOf(\PDOStatement::class, $statement);

        /** @var list<string> */
        return $statement->fetchAll(PDO::FETCH_COLUMN);
    }
}
