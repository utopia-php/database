<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Conflict as ConflictException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Timeout as TimeoutException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

trait JoinsTests
{
    /**
     * @throws AuthorizationException
     * @throws ConflictException
     * @throws TimeoutException
     * @throws DuplicateException
     * @throws LimitException
     * @throws StructureException
     * @throws DatabaseException
     * @throws QueryException
     */
    public function testJoin(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        Authorization::setRole('user:bob');

        static::getDatabase()->createCollection('__users');
        static::getDatabase()->createCollection('__sessions');

        static::getDatabase()->createAttribute('__users', 'username', Database::VAR_STRING, 100, false);
        static::getDatabase()->createAttribute('__sessions', 'user_id', Database::VAR_STRING, 100, false);
        static::getDatabase()->createAttribute('__sessions', 'boolean', Database::VAR_BOOLEAN, 0, false);
        static::getDatabase()->createAttribute('__sessions', 'float', Database::VAR_FLOAT, 0, false);

        $user1 = static::getDatabase()->createDocument('__users', new Document([
            'username' => 'Donald',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('bob')),
            ],
        ]));

        $session1 = static::getDatabase()->createDocument('__sessions', new Document([
            'user_id' => $user1->getId(),
            '$permissions' => [],
        ]));

        /**
         * Test $session1 does not have read permissions
         * Test right attribute is internal attribute
         */
        $documents = static::getDatabase()->find(
            '__users',
            [
                Query::join(
                    '__sessions',
                    'B',
                    [
                        Query::relationEqual('B', 'user_id', '', '$id'),
                    ]
                ),
            ]
        );
        $this->assertCount(0, $documents);

        $session2 = static::getDatabase()->createDocument('__sessions', new Document([
            'user_id' => $user1->getId(),
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'boolean' => false,
            'float' => 10.5,
        ]));

        $user2 = static::getDatabase()->createDocument('__users', new Document([
            'username' => 'Abraham',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('bob')),
            ],
        ]));

        $session3 = static::getDatabase()->createDocument('__sessions', new Document([
            'user_id' => $user2->getId(),
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'boolean' => true,
            'float' => 5.5,
        ]));

        /**
         * Test $session2 has read permissions
         * Test right attribute is internal attribute
         */
        $documents = static::getDatabase()->find(
            '__users',
            [
                Query::join(
                    '__sessions',
                    'B',
                    [
                        Query::relationEqual('B', 'user_id', '', '$id'),
                    ]
                ),
            ]
        );
        $this->assertCount(2, $documents);

        $documents = static::getDatabase()->find(
            '__users',
            [
                Query::join(
                    '__sessions',
                    'B',
                    [
                        Query::relationEqual('B', 'user_id', '', '$id'),
                        Query::equal('user_id', [$user1->getId()], 'B'),
                    ]
                ),
            ]
        );
        $this->assertCount(1, $documents);

        /**
         * Test alias does not exist
         */
        try {
            static::getDatabase()->find(
                '__sessions',
                [
                    Query::equal('user_id', ['bob'], 'alias_not_found')
                ]
            );
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Invalid query: Unknown Alias context', $e->getMessage());
        }

        /**
         * Test Ambiguous alias
         */
        try {
            static::getDatabase()->find(
                '__users',
                [
                    Query::join('__sessions', Query::DEFAULT_ALIAS, []),
                ]
            );
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Ambiguous alias for collection "__sessions".', $e->getMessage());
        }

        /**
         * Test relation query exist, but not on the join alias
         */
        try {
            static::getDatabase()->find(
                '__users',
                [
                    Query::join(
                        '__sessions',
                        'B',
                        [
                            Query::relationEqual('', '$id', '', '$id'),
                        ]
                    ),
                ]
            );
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Invalid query: At least one relation query is required on the joined collection.', $e->getMessage());
        }

        /**
         * Test if relation query exists in the join queries list
         */
        try {
            static::getDatabase()->find(
                '__users',
                [
                    Query::join('__sessions', 'B', []),
                ]
            );
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Invalid query: At least one relation query is required on the joined collection.', $e->getMessage());
        }

        /**
         * Test allow only filter queries in joins ON clause
         */
        try {
            static::getDatabase()->find(
                '__users',
                [
                    Query::join('__sessions', 'B', [
                        Query::orderAsc()
                    ]),
                ]
            );
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Invalid query: InnerJoin queries can only contain filter queries', $e->getMessage());
        }

        /**
         * Test Relations are valid within joins
         */
        try {
            static::getDatabase()->find(
                '__users',
                [
                    Query::relationEqual('', '$id', '', '$internalId'),
                ]
            );
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Invalid query: Relations are only valid within joins.', $e->getMessage());
        }

        /**
         * Test invalid alias name
         */
        try {
            $alias = 'drop schema;';
            static::getDatabase()->find(
                '__users',
                [
                    Query::join(
                        '__sessions',
                        $alias,
                        [
                            Query::relationEqual($alias, 'user_id', '', '$id'),
                        ]
                    ),
                ]
            );
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Query InnerJoin: Alias must contain at most 64 chars. Valid chars are a-z, A-Z, 0-9, and underscore.', $e->getMessage());
        }

        /**
         * Test join same collection
         */
        $documents = static::getDatabase()->find(
            '__users',
            [
                Query::join(
                    '__sessions',
                    'B',
                    [
                        Query::relationEqual('B', 'user_id', '', '$id'),
                    ]
                ),
                Query::join(
                    '__sessions',
                    'C',
                    [
                        Query::relationEqual('C', 'user_id', 'B', 'user_id'),
                    ]
                ),
            ]
        );
        $this->assertCount(2, $documents);

        /**
         * Test order by related collection
         */
        $documents = static::getDatabase()->find(
            '__users',
            [
                Query::join(
                    '__sessions',
                    'B',
                    [
                        Query::relationEqual('B', 'user_id', '', '$id'),
                    ]
                ),
                Query::orderAsc('$createdAt', 'B')
            ]
        );
        $this->assertEquals('Donald', $documents[0]['username']);
        $this->assertEquals('Abraham', $documents[1]['username']);

        $documents = static::getDatabase()->find(
            '__users',
            [
                Query::join(
                    '__sessions',
                    'B',
                    [
                        Query::relationEqual('B', 'user_id', '', '$id'),
                    ]
                ),
                Query::orderDesc('$createdAt', 'B')
            ]
        );
        $this->assertEquals('Abraham', $documents[0]['username']);
        $this->assertEquals('Donald', $documents[1]['username']);

        /**
         * Select queries
         */
        $documents = static::getDatabase()->find(
            '__users',
            [
                Query::select('*', 'main'),
                Query::select('$id', 'main'),
                Query::select('user_id', 'S', as: 'we need to support this'),
                Query::select('float', 'S'),
                Query::select('boolean', 'S'),
                Query::select('*', 'S'),
                Query::join(
                    '__sessions',
                    'S',
                    [
                        Query::relationEqual('', '$id', 'S', 'user_id'),
                        Query::greaterThan('float', 1.1, 'S'),
                    ]
                ),
                Query::orderDesc('float', 'S'),
            ]
        );

        $document = $documents[0];
        var_dump($document);
        $this->assertIsFloat($document->getAttribute('float'));
        $this->assertEquals(5.5, $document->getAttribute('float'));

        $this->assertIsBool($document->getAttribute('boolean'));
        $this->assertEquals(true, $document->getAttribute('boolean'));
        //$this->assertIsArray($document->getAttribute('colors'));
        //$this->assertEquals(['pink', 'green', 'blue'], $document->getAttribute('colors'));

        //$this->assertEquals('shmuel1', 'shmuel2');
    }
}
