<?php

namespace Tests\Unit\Documents;

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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;

/**
 * Under tenant-per-document two tenants can each hold a document with the same id, so an
 * upsert batch spanning tenants must hand every document the sequence of its own tenant's row.
 */
final class TenantSequencesTest extends TestCase
{
    private const string COLLECTION = 'notes';

    private const int TENANT = 1;

    private const int OTHER_TENANT = 2;

    private Database $database;

    /**
     * @return array<string, array{bool}>
     */
    public static function fetchModes(): array
    {
        return [
            'native fetches' => [false],
            'stringified fetches' => [true],
        ];
    }

    #[DataProvider('fetchModes')]
    public function testAnUpsertReportsEachTenantsOwnSequenceForTheSameNewId(bool $stringifyFetches): void
    {
        $this->database = $this->database($stringifyFetches);

        $reported = $this->upsert([
            $this->note(self::TENANT, 'shared'),
            $this->note(self::OTHER_TENANT, 'shared'),
        ]);

        $this->assertSame($this->stored(['shared' => [self::TENANT, self::OTHER_TENANT]]), $reported);
        $this->assertNotSame($reported[self::TENANT]['shared'], $reported[self::OTHER_TENANT]['shared']);
    }

    #[DataProvider('fetchModes')]
    public function testAnUpsertOfThreeDocumentsOverTwoTenantsReportsEachDocumentsOwnSequence(bool $stringifyFetches): void
    {
        $this->database = $this->database($stringifyFetches);
        $this->database->createDocument(self::COLLECTION, $this->note(self::OTHER_TENANT, 'existing'));

        $reported = $this->upsert([
            $this->note(self::OTHER_TENANT, 'shared'),
            $this->note(self::TENANT, 'shared'),
            $this->note(self::OTHER_TENANT, 'existing', 'renamed'),
        ]);

        $this->assertSame(
            $this->stored(['shared' => [self::TENANT, self::OTHER_TENANT], 'existing' => [self::OTHER_TENANT]]),
            $reported,
        );
    }

    public function testATenantGivenAsADigitStringMatchesItsIntegerRow(): void
    {
        $this->database = $this->database(false);

        $reported = $this->upsert([
            $this->note(self::TENANT, 'shared'),
            $this->note((string) self::OTHER_TENANT, 'shared'),
        ]);

        $this->assertSame($this->stored(['shared' => [self::TENANT, self::OTHER_TENANT]]), $reported);
    }

    private function database(bool $stringifyFetches): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = (new Database(
            new SQLite(new PDO('sqlite::memory:', options: [PDO::ATTR_STRINGIFY_FETCHES => $stringifyFetches])),
            new Cache(new None()),
        ))
            ->setAuthorization($authorization)
            ->setDatabase('tenant_sequences')
            ->setNamespace('tenant_sequences')
            ->setSharedTables(true)
            ->setTenant(null)
            ->setTenantPerDocument(true);
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64, required: false)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ));

        return $database;
    }

    private function note(int|string $tenant, string $id, string $title = 'note'): Document
    {
        return new Document([
            Document::ID => $id,
            Document::TENANT => $tenant,
            'title' => $title,
        ]);
    }

    /**
     * @param  array<Document>  $documents
     * @return array<int, array<string, string>>
     */
    private function upsert(array $documents): array
    {
        $reported = [];
        $this->database->upsertDocuments(
            self::COLLECTION,
            $documents,
            onNext: function (Document $document) use (&$reported): void {
                $reported[(int) $document->getTenant()][$document->getId()] = (string) $document->getSequence();
            },
        );

        return $this->sorted($reported);
    }

    /**
     * @param  array<string, list<int>>  $tenantsById
     * @return array<int, array<string, string>>
     */
    private function stored(array $tenantsById): array
    {
        $stored = [];
        foreach ($tenantsById as $id => $tenants) {
            foreach ($tenants as $tenant) {
                $document = $this->database->withTenant(
                    $tenant,
                    fn (): Document => $this->database->getDocument(self::COLLECTION, $id),
                );
                $this->assertFalse($document->isEmpty(), "Tenant {$tenant} must hold '{$id}'");
                $stored[$tenant][$id] = (string) $document->getSequence();
            }
        }

        return $this->sorted($stored);
    }

    /**
     * @param  array<int, array<string, string>>  $sequences
     * @return array<int, array<string, string>>
     */
    private function sorted(array $sequences): array
    {
        \ksort($sequences);
        foreach ($sequences as &$byId) {
            \ksort($byId);
        }

        return $sequences;
    }
}
