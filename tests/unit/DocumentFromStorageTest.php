<?php

namespace Tests\Unit;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
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
