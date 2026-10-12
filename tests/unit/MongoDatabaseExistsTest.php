<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use stdClass;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Mongo;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Mongo\Client;

final class MongoDatabaseExistsTest extends TestCase
{
    public function testAListedDatabaseExists(): void
    {
        $this->assertTrue($this->adapter(['admin', 'library'])->exists('library'));
    }

    public function testAnUnlistedDatabaseDoesNotExist(): void
    {
        $this->assertFalse($this->adapter(['admin', 'library'])->exists('archive'));
    }

    public function testNoDatabaseExistsOnAnEmptyServer(): void
    {
        $this->assertFalse($this->adapter([])->exists('library'));
    }

    public function testTheNameIsFilteredBeforeLookup(): void
    {
        $this->assertTrue($this->adapter(['library'])->exists('lib.rary'));
    }

    public function testARefusedRenameUnderSharedTablesLeavesNoDatabaseBehind(): void
    {
        $adapter = $this->adapter(['library']);
        $adapter->setSharedTables(true);
        $database = new Database($adapter, new Cache(new None()));
        $database->setDatabase('library');

        try {
            $database->update('library', 'archive');
            $this->fail('A rename under shared tables must be refused');
        } catch (DatabaseException $error) {
            $this->assertSame('Cannot rename a database while shared tables are enabled', $error->getMessage());
        }

        $this->assertSame('library', $database->getDatabase());
        $this->assertTrue($database->exists('library'));
        $this->assertFalse($database->exists('archive'));
    }

    /**
     * @param  list<string>  $databases
     */
    private function adapter(array $databases): Mongo
    {
        $client = new class ($databases) extends Client {
            /**
             * @param  list<string>  $databases
             */
            public function __construct(private readonly array $databases)
            {
            }

            #[\Override]
            public function connect(): self
            {
                return $this;
            }

            #[\Override]
            public function close(): void
            {
            }

            #[\Override]
            public function listDatabaseNames(): stdClass
            {
                $listed = new stdClass();
                $listed->databases = \array_map(static function (string $name): stdClass {
                    $database = new stdClass();
                    $database->name = $name;

                    return $database;
                }, $this->databases);

                return $listed;
            }
        };

        return new Mongo($client);
    }
}
