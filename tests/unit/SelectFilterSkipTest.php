<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory as CacheMemory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory as DatabaseMemory;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

class SelectFilterSkipTest extends TestCase
{
    public function testFilterNotAppliedWhenAttributeNotSelected(): void
    {
        $database = new Database(new DatabaseMemory(), new Cache(new CacheMemory()));
        $database
            ->setDatabase('utopiaTests')
            ->setNamespace('select_filter_'.\uniqid());

        $database->create();

        $calls = 0;
        $database->addFilter(
            'subQueryProbeUnit',
            fn (mixed $value) => null,
            function (mixed $value) use (&$calls) {
                $calls++;

                return ['fanned', 'out'];
            }
        );

        $database->createCollection('filterSelect');
        $database->createAttribute('filterSelect', new Attribute(key: 'plain', type: ColumnType::String, size: 128, required: false));
        $database->createAttribute('filterSelect', new Attribute(key: 'kids', type: ColumnType::String, size: 128, required: false, filters: ['subQueryProbeUnit']));

        $database->createDocument('filterSelect', new Document([
            '$id' => 'doc1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'plain' => 'x',
        ]));

        $calls = 0;
        $document = $database->getDocument('filterSelect', 'doc1');
        $this->assertSame(1, $calls);
        $this->assertSame(['fanned', 'out'], $document->getAttribute('kids'));

        $calls = 0;
        $document = $database->getDocument('filterSelect', 'doc1', [Query::select(['$id', 'plain'])]);
        $this->assertSame(0, $calls);
        $this->assertNull($document->getAttribute('kids'));
        $this->assertSame('x', $document->getAttribute('plain'));

        $calls = 0;
        $document = $database->getDocument('filterSelect', 'doc1', [Query::select(['$id', 'kids'])]);
        $this->assertSame(1, $calls);
        $this->assertSame(['fanned', 'out'], $document->getAttribute('kids'));

        $calls = 0;
        $document = $database->getDocument('filterSelect', 'doc1', [Query::select(['*'])]);
        $this->assertSame(1, $calls);
        $this->assertSame(['fanned', 'out'], $document->getAttribute('kids'));

        $calls = 0;
        $documents = $database->find('filterSelect', [Query::select(['$id', 'plain'])]);
        $this->assertCount(1, $documents);
        $this->assertSame(0, $calls);
        $this->assertNull($documents[0]->getAttribute('kids'));

        $calls = 0;
        $documents = $database->find('filterSelect');
        $this->assertCount(1, $documents);
        $this->assertSame(1, $calls);
        $this->assertSame(['fanned', 'out'], $documents[0]->getAttribute('kids'));
    }
}
