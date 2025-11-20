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
        /**
         * @var Database $db
         */
        $db = static::getDatabase();

        if (!$db->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        //Authorization::setRole('user:bob');

        $db->createCollection('__users');
        $db->createCollection('__sessions');

        $db->createAttribute('__users', 'username', Database::VAR_STRING, 100, false);
        $db->createAttribute('__sessions', 'user_id', Database::VAR_STRING, 100, false);
        $db->createAttribute('__sessions', 'boolean', Database::VAR_BOOLEAN, 0, false);
        $db->createAttribute('__sessions', 'float', Database::VAR_FLOAT, 0, false);

        $user1 = $db->createDocument('__users', new Document([
            'username' => 'Donald',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('bob')),
            ],
        ]));

        $sessionNoPermissions = $db->createDocument('__sessions', new Document([
            'user_id' => $user1->getId(),
            '$permissions' => [],
        ]));

        /**
         * Test $session1 does not have read permissions
         * Test right attribute is internal attribute
         */
        $documents = $db->find(
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

        $session2 = $db->createDocument('__sessions', new Document([
            'user_id' => $user1->getId(),
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'boolean' => false,
            'float' => 10.5,
        ]));

        $user2 = $db->createDocument('__users', new Document([
            'username' => 'Abraham',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('bob')),
            ],
        ]));

        $session3 = $db->createDocument('__sessions', new Document([
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
        $documents = $db->find(
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

        $documents = $db->find(
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
            $db->find(
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
            $db->find(
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
            $db->find(
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
            $db->find(
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
            $db->find(
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
            $db->find(
                '__users',
                [
                    Query::relationEqual('', '$id', '', '$sequence'),
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
            $db->find(
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
        $documents = $db->find(
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
        $documents = $db->find(
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

        $documents = $db->find(
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
        $documents = $db->find(
            '__users',
            [
                Query::select('*', 'main'),
                Query::select('user_id', 'S'),
                Query::select('float', 'S'),
                Query::select('boolean', 'S'),
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

        /**
         * Since we use main.* we should see all attributes
         */
        $document = $documents[0];
        $this->assertArrayHasKey('$id', $document);
        $this->assertIsFloat($document->getAttribute('float'));
        $this->assertEquals(10.5, $document->getAttribute('float'));
        $this->assertIsBool($document->getAttribute('boolean'));
        $this->assertEquals(false, $document->getAttribute('boolean'));

        /**
         * Test invalid as
         */
        try {
            $db->find('__users', [
                Query::select('$id', as: 'truncate schema;'),
            ]);
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Invalid Query Select: "as" must contain at most 64 chars. Valid chars are a-z, A-Z, 0-9, and underscore.', $e->getMessage());
        }

        try {
            $db->find('__users', [
                Query::select('*', as: 'as'),
            ]);
            $this->fail('Failed to throw exception');
        } catch (\Throwable $e) {
            $this->assertTrue($e instanceof QueryException);
            $this->assertEquals('Invalid Query Select: Invalid "as" on attribute "*"', $e->getMessage());
        }

        /**
         * Simple `as` query getDocument
         */
        $document = $db->getDocument(
            '__sessions',
            $session2->getId(),
            [
                Query::select('$permissions', as: '___permissions'),
                Query::select('$id', as: '___uid'),
                Query::select('$sequence', as: '___id'),
                Query::select('$createdAt', as: '___created'),
                Query::select('user_id', as: 'user_id_as'),
                Query::select('float', as: 'float_as'),
                Query::select('boolean', as: 'boolean_as'),
            ]
        );

        $this->assertArrayNotHasKey('$permissions', $document);
        $this->assertArrayHasKey('___permissions', $document);
        $this->assertArrayHasKey('___uid', $document);
        //$this->assertArrayNotHasKey('$id', $document);
        $this->assertArrayHasKey('___id', $document);
        $this->assertArrayNotHasKey('$sequence', $document);
        $this->assertArrayHasKey('___created', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayHasKey('user_id_as', $document);
        $this->assertArrayNotHasKey('user_id', $document);
        $this->assertArrayHasKey('float_as', $document);
        $this->assertArrayNotHasKey('float', $document);
        $this->assertIsFloat($document->getAttribute('float_as'));
        $this->assertEquals(10.5, $document->getAttribute('float_as'));
        $this->assertArrayHasKey('boolean_as', $document);
        $this->assertArrayNotHasKey('boolean', $document);
        $this->assertIsBool($document->getAttribute('boolean_as'));
        $this->assertEquals(false, $document->getAttribute('boolean_as'));

        /**
         * Simple `as` query getDocument
         */
        $document = $db->getDocument(
            '__sessions',
            $session2->getId(),
            [
                Query::select('$permissions', as: '___permissions'),
            ]
        );
        $this->assertArrayHasKey('___permissions', $document);
        $this->assertArrayNotHasKey('$permissions', $document);
        //$this->assertArrayNotHasKey('$id', $document); // Added in processRelationshipQueries
        $this->assertArrayHasKey('$collection', $document);

        /**
         * Simple `as` query find
         */
        $document = $db->findOne(
            '__sessions',
            [
                Query::select('$id', as: '___uid'),
                Query::select('$sequence', as: '___id'),
                Query::select('$createdAt', as: '___created'),
                Query::select('user_id', as: 'user_id_as'),
                Query::select('float', as: 'float_as'),
                Query::select('boolean', as: 'boolean_as'),
            ]
        );

        $this->assertArrayHasKey('___uid', $document);
        //$this->assertArrayNotHasKey('$id', $document); // Added in processRelationshipQueries
        $this->assertArrayHasKey('___id', $document);
        $this->assertArrayNotHasKey('$sequence', $document);
        $this->assertArrayHasKey('___created', $document);
        $this->assertArrayNotHasKey('$createdAt', $document);
        $this->assertArrayHasKey('user_id_as', $document);
        $this->assertArrayNotHasKey('user_id', $document);
        $this->assertArrayHasKey('float_as', $document);
        $this->assertArrayNotHasKey('float', $document);
        $this->assertIsFloat($document->getAttribute('float_as'));
        $this->assertEquals(10.5, $document->getAttribute('float_as'));
        $this->assertArrayHasKey('boolean_as', $document);
        $this->assertArrayNotHasKey('boolean', $document);
        $this->assertIsBool($document->getAttribute('boolean_as'));
        $this->assertEquals(false, $document->getAttribute('boolean_as'));

        /**
         * Select queries
         */
        $document = $db->findOne(
            '__users',
            [
                Query::select('username', '', as: 'as_username'),
                Query::select('user_id', 'S', as: 'as_user_id'),
                Query::select('float', 'S', as: 'as_float'),
                Query::select('boolean', 'S', as: 'as_boolean'),
                Query::select('$permissions', 'S', as: 'as_permissions'),
                Query::join(
                    '__sessions',
                    'S',
                    [
                        Query::relationEqual('', '$id', 'S', 'user_id'),
                    ]
                )
            ]
        );

        $this->assertArrayHasKey('as_username', $document);
        $this->assertArrayHasKey('as_user_id', $document);
        $this->assertArrayHasKey('as_float', $document);
        $this->assertArrayHasKey('as_boolean', $document);
        $this->assertArrayHasKey('as_permissions', $document);
        $this->assertIsArray($document->getAttribute('as_permissions'));



        //        /**
        //         * ambiguous and duplications selects
        //         */
        //        try {
        //            $db->find(
        //                '__users',
        //                [
        //                    Query::select('$id', 'main'),
        //                    Query::select('$id', 'S'),
        //                    Query::join('__sessions', 'S',
        //                        [
        //                            Query::relationEqual('', '$id', 'S', 'user_id'),
        //                        ]
        //                    )
        //                ]
        //            );
        //            $this->fail('Failed to throw exception');
        //        } catch (\Throwable $e) {
        //            $this->assertTrue($e instanceof QueryException);
        //            $this->assertEquals('Invalid Query Select: ambiguous column "$id"', $e->getMessage());
        //        }
        //
        //        try {
        //            $db->find(
        //                '__users',
        //                [
        //                    Query::select('*', 'main'),
        //                    Query::select('*', 'S'),
        //                    Query::join('__sessions', 'S',
        //                        [
        //                            Query::relationEqual('', '$id', 'S', 'user_id'),
        //                        ]
        //                    )
        //                ]
        //            );
        //            $this->fail('Failed to throw exception');
        //        } catch (\Throwable $e) {
        //            $this->assertTrue($e instanceof QueryException);
        //            $this->assertEquals('Invalid Query Select: ambiguous column "*"', $e->getMessage());
        //        }
        //
        //        try {
        //            $db->find('__users',
        //                [
        //                    Query::select('$id'),
        //                    Query::select('$id'),
        //                ]
        //            );
        //            $this->fail('Failed to throw exception');
        //        } catch (\Throwable $e) {
        //            $this->assertTrue($e instanceof QueryException);
        //            $this->assertEquals('Duplicate Query Select on "main.$id"', $e->getMessage());
        //        }
        //
        //        /**
        //         * This should fail? since 2 _uid attributes will be returned?
        //         */
        //        try {
        //            $db->find(
        //                '__users',
        //                [
        //                    Query::select('*', 'main'),
        //                    Query::select('$id', 'S'),
        //                    Query::join('__sessions', 'S',
        //                        [
        //                            Query::relationEqual('', '$id', 'S', 'user_id'),
        //                        ]
        //                    )
        //                ]
        //            );
        //            $this->fail('Failed to throw exception');
        //        } catch (\Throwable $e) {
        //            $this->assertTrue($e instanceof QueryException);
        //            $this->assertEquals('Invalid Query Select: ambiguous column "$id"', $e->getMessage());
        //        }
    }
}
