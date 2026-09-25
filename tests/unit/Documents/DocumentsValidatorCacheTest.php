<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Mirror;
use Utopia\Database\Validator\Queries\Documents as DocumentsValidator;

class DocumentsValidatorCacheTest extends TestCase
{
    private Document $orders;

    private Document $customers;

    protected function setUp(): void
    {
        $this->orders = new Document([
            '$id' => 'orders',
            'attributes' => [],
            'indexes' => [],
        ]);
        $this->customers = new Document([
            '$id' => 'customers',
            'attributes' => [],
            'indexes' => [],
        ]);
    }

    public function testValidatorIsReusedWithoutJoinedCollections(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));

        $this->assertSame(
            $this->documentsValidator($database, $this->orders),
            $this->documentsValidator($database, $this->orders),
        );
    }

    public function testJoinedCollectionsBypassTheCache(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));

        $cached = $this->documentsValidator($database, $this->orders);
        $joined = $this->documentsValidator($database, $this->orders, [$this->customers]);

        $this->assertNotSame($cached, $joined);
        $this->assertNotSame($joined, $this->documentsValidator($database, $this->orders, [$this->customers]));
        $this->assertSame($cached, $this->documentsValidator($database, $this->orders));
    }

    public function testMirrorForwardsJoinedCollectionsToTheSource(): void
    {
        $mirror = new Mirror(new Database(new Memory(), new Cache(new None())));

        $cached = $this->documentsValidator($mirror, $this->orders);

        $this->assertSame($cached, $this->documentsValidator($mirror, $this->orders));
        $this->assertNotSame($cached, $this->documentsValidator($mirror, $this->orders, [$this->customers]));
    }

    /**
     * @param  array<Document>  $joinedCollections
     */
    private function documentsValidator(Database $database, Document $collection, array $joinedCollections = []): DocumentsValidator
    {
        $validator = (new ReflectionMethod($database, 'getDocumentsValidator'))->invoke($database, $collection, $joinedCollections);
        $this->assertInstanceOf(DocumentsValidator::class, $validator);

        return $validator;
    }
}
