<?php

namespace Tests\Unit\Attributes;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Truncate as TruncateException;
use Utopia\Database\Validator\Authorization;

/**
 * Under MySQL emulation SQLite refuses a smaller string size that existing values exceed, as
 * MariaDB does. A new key in the same update must not skip that check, and the refusal must
 * leave the column where it was.
 */
final class RenameShrinkTest extends TestCase
{
    private const string COLLECTION = 'notes';

    /**
     * @return array<string, array{bool}>
     */
    public static function tenancies(): array
    {
        return [
            'dedicated tables' => [false],
            'shared tables' => [true],
        ];
    }

    #[DataProvider('tenancies')]
    public function testARenameThatShrinksBelowAStoredValueIsRefused(bool $sharedTables): void
    {
        $database = $this->createDatabase($sharedTables);

        try {
            $database->updateAttribute(self::COLLECTION, 'title', size: 4, newKey: 'heading');
            $this->fail('A size existing values exceed must be refused with a new key too');
        } catch (TruncateException $error) {
            $this->assertSame("Attribute 'title' has values exceeding new size 4", $error->getMessage());
        }

        $document = $database->getDocument(self::COLLECTION, 'note');
        $this->assertSame('a long title', $document->getAttribute('title'), 'The refused update must leave the column under its old key');
        $this->assertSame(['title'], $this->keys($database));
    }

    #[DataProvider('tenancies')]
    public function testARenameThatShrinksAboveEveryStoredValueRuns(bool $sharedTables): void
    {
        $database = $this->createDatabase($sharedTables);

        $this->assertSame('heading', $database->updateAttribute(self::COLLECTION, 'title', size: 32, newKey: 'heading')->getId());

        $this->assertSame('a long title', $database->getDocument(self::COLLECTION, 'note')->getAttribute('heading'));
    }

    public function testATenantAdoptingAnotherTenantsRenameChecksTheRenamedColumn(): void
    {
        $database = $this->createDatabase(true);
        $database->setTenant(2);
        $database->createCollection($this->definition());
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'note', 'title' => 'a much longer title']));

        $database->setTenant(1);
        $database->updateAttribute(self::COLLECTION, 'title', newKey: 'heading');

        $database->setTenant(2);
        try {
            $database->updateAttribute(self::COLLECTION, 'title', size: 16, newKey: 'heading');
            $this->fail('The values under the renamed column must be checked');
        } catch (TruncateException) {
            $this->addToAssertionCount(1);
        }

        $this->assertSame('heading', $database->updateAttribute(self::COLLECTION, 'title', size: 32, newKey: 'heading')->getId());
        $this->assertSame('a much longer title', $database->getDocument(self::COLLECTION, 'note')->getAttribute('heading'));
    }

    /**
     * @return list<string>
     */
    private function keys(Database $database): array
    {
        return \array_map(static fn (Attribute $attribute): string => $attribute->key, \array_values($database->getCollection(self::COLLECTION)->attributes));
    }

    private function definition(): Collection
    {
        return new Collection(id: self::COLLECTION, attributes: [Attribute::string(key: 'title', size: 64)]);
    }

    private function createDatabase(bool $sharedTables): Database
    {
        $adapter = new SQLite(new PDO('sqlite::memory:'));
        $adapter->setEmulateMySQL(true);
        $authorization = new Authorization();
        $authorization->disable();

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace('shrink')
            ->setSharedTables($sharedTables)
            ->setTenant($sharedTables ? 1 : null);
        $database->create();
        $database->createCollection($this->definition());
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'note', 'title' => 'a long title']));

        return $database;
    }
}
