<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;

final class DocumentsValidatorFingerprintTest extends TestCase
{
    public function testAnEqualSchemaReusesTheValidator(): void
    {
        $database = new DocumentsValidatorDatabase(new Memory(), new Cache(new None()));

        $this->assertSame(
            $database->documentsValidator($this->collection()),
            $database->documentsValidator($this->collection()),
        );
        $this->assertSame(
            $database->documentsValidator($this->collection()),
            $database->documentsValidator(clone $this->collection()),
        );
    }

    public function testEveryPartOfTheSchemaSelectsItsOwnValidator(): void
    {
        $database = new DocumentsValidatorDatabase(new Memory(), new Cache(new None()));
        $original = $database->documentsValidator($this->collection());

        $renamed = $this->collection();
        $title = $renamed->attributes[0];
        $title->key = 'author';
        $resized = $this->collection();
        $resized->attributes[0]->setAttribute('size', 32);
        $added = $this->collection();
        $added->setAttribute('attributes', [...$added->attributes, Attribute::integer(key: 'pages')]);
        $indexed = $this->collection();
        $indexed->setAttribute('indexes', []);
        $permitted = $this->collection();
        $permitted->setAttribute('$permissions', [Permission::read(Role::users())]);
        $secured = $this->collection();
        $secured->setAttribute('documentSecurity', true);

        foreach (['renamed' => $renamed, 'resized' => $resized, 'added' => $added, 'indexed' => $indexed, 'permitted' => $permitted, 'secured' => $secured] as $change => $collection) {
            $this->assertNotSame($original, $database->documentsValidator($collection), $change);
        }

        $this->assertTrue($database->documentsValidator($renamed)->isValid([Query::equal('author', ['ada'])]));
        $this->assertFalse($database->documentsValidator($renamed)->isValid([Query::equal('title', ['ada'])]));
        $this->assertTrue($database->documentsValidator($added)->isValid([Query::equal('pages', [1])]));
        $this->assertFalse($database->documentsValidator($this->collection())->isValid([Query::equal('pages', [1])]));
    }

    private function collection(): Collection
    {
        return new Collection(
            id: 'books',
            attributes: [Attribute::string(key: 'title', size: 64)],
            indexes: [Index::key(key: 'by_title', attributes: ['title'])],
            permissions: [Permission::read(Role::any())],
            documentSecurity: false,
        );
    }
}
