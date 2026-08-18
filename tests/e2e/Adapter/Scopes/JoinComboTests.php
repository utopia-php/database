<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

trait JoinComboTests
{
    public function testJoinComboLeftPublicAndInnerSecretMixedDocSec(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $pubCol, $secCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $pubCol, $secCol): void {
            $results = $database->find($mCol, [
                Query::leftJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::join($secCol, '$id', 'mainId', '=', 'sec'),
            ]);

            $this->assertSame(5, \count($results));
            $this->assertComboSecretsHidden($results);

            $scores = $this->comboNumericScores($results);
            $this->assertContains(313, $scores);
            $this->assertContains(10, $scores);
            $this->assertSame(false, \in_array(777, $scores, true));
            $this->assertSame(false, \in_array(4242, $scores, true));

            $ids = \array_map(static fn (Document $document): string => $document->getId(), $results);
            $this->assertContains('m1', $ids);
            $this->assertContains('m2', $ids);
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboSelfJoinPlusThirdTableAclPerAlias(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [, , , $selfCol, $cCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $selfCol, $cCol): void {
            $results = $database->find($selfCol, [
                Query::join($selfCol, 'tag', 'tag', '=', 'visible'),
                Query::join($selfCol, 'tag', 'tag', '=', 'hidden'),
                Query::join($cCol, 'visible.$id', 'selfId', '=', 'c'),
                Query::select(['visible.payload', 'hidden.payload', 'c.secret']),
            ]);

            $this->assertSame(1, \count($results));
            $this->assertComboSecretsHidden($results);
            $this->assertSame('open-payload', $results[0]->getAttribute('payload'));
            $this->assertNotSame('combo-secret-alpha', $results[0]->getAttribute('payload'));
            $this->assertSame('c-open-token', $results[0]->getAttribute('secret'));
            $this->assertNotSame('c-combo-secret', $results[0]->getId());
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboOrderBySecretColumnLimitOffsetOracle(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $pubCol, $secCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $pubCol, $secCol): void {
            $ordered = $database->skipValidation(fn () => $database->find($mCol, [
                Query::leftJoin($secCol, '$id', 'mainId', '=', 'sec'),
                Query::orderDesc('sec.score'),
            ]));
            $this->assertComboSecretsHidden($ordered);
            $scores = $this->comboNumericScores($ordered);
            $this->assertSame(false, \in_array(777, $scores, true));
            $this->assertContains(313, $scores);
            $this->assertSame($scores, $this->sortedDesc($scores));

            $limited = $database->skipValidation(fn () => $database->find($mCol, [
                Query::leftJoin($secCol, '$id', 'mainId', '=', 'sec'),
                Query::orderDesc('sec.score'),
                Query::limit(2),
            ]));
            $this->assertSame(2, \count($limited));
            $this->assertComboSecretsHidden($limited);
            $this->assertSame(
                \array_slice($this->comboNumericScores($ordered), 0, 2),
                $this->comboNumericScores($limited)
            );

            $offset = $database->skipValidation(fn () => $database->find($mCol, [
                Query::leftJoin($secCol, '$id', 'mainId', '=', 'sec'),
                Query::orderDesc('sec.score'),
                Query::limit(2),
                Query::offset(1),
            ]));
            $this->assertSame(2, \count($offset));
            $this->assertComboSecretsHidden($offset);
            $this->assertSame(
                \array_slice($this->comboNumericScores($ordered), 1, 2),
                $this->comboNumericScores($offset)
            );

            $foj = $database->skipValidation(fn () => $database->find($mCol, [
                Query::fullOuterJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::orderDesc('pub.score'),
            ]));
            $this->assertComboSecretsHidden($foj);
            $fojScores = $this->comboNumericScores($foj);
            $this->assertSame(4242, $fojScores[0]);
            $this->assertContains(313, $fojScores);
            $this->assertSame(false, \in_array(777, $fojScores, true));
            $this->assertSame($fojScores, $this->sortedDesc($fojScores));

            $fojLimited = $database->skipValidation(fn () => $database->find($mCol, [
                Query::fullOuterJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::orderDesc('pub.score'),
                Query::limit(2),
                Query::offset(1),
            ]));
            $this->assertSame(2, \count($fojLimited));
            $this->assertComboSecretsHidden($fojLimited);

            $identity = static function (Document $document): string {
                $score = $document->getAttribute('pub.score') ?? $document->getAttribute('score');

                return $document->getId().':'.(\is_numeric($score) ? (string) (int) $score : '');
            };
            $this->assertSame(
                \array_slice(\array_map($identity, $foj), 1, 2),
                \array_map($identity, $fojLimited)
            );
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboFilterSecretWithoutProjectingSecret(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, , $secCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $secCol): void {
            $results = $database->skipValidation(fn () => $database->find($mCol, [
                Query::join($secCol, '$id', 'mainId', '=', 'rev'),
                Query::equal('rev.score', [777]),
                Query::select(['name']),
            ]));

            $this->assertSame(0, \count($results));
            $this->assertComboSecretsHidden($results);
            foreach ($results as $document) {
                $this->assertSame(null, $document->getAttribute('score'));
                $this->assertSame(null, $document->getAttribute('secret'));
            }
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboSumCountHavingExactSiblingOracle(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, , $secCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $secCol): void {
            $aggregated = $database->skipValidation(fn () => $database->find($mCol, [
                Query::join($secCol, '$id', 'mainId', '=', 'rev'),
                Query::equal('name', ['Main']),
                Query::sum('rev.score', 'total'),
                Query::count('*', 'cnt'),
                Query::groupBy(['name']),
            ]));

            $this->assertSame(1, \count($aggregated));
            $this->assertComboSecretsHidden($aggregated);
            $this->assertSame(323, (int) $aggregated[0]->getAttribute('total'));
            $this->assertNotSame(1100, (int) $aggregated[0]->getAttribute('total'));
            $this->assertSame(2, (int) $aggregated[0]->getAttribute('cnt'));
            $this->assertNotSame(3, (int) $aggregated[0]->getAttribute('cnt'));

            $havingSum = $database->skipValidation(fn () => $database->find($mCol, [
                Query::join($secCol, '$id', 'mainId', '=', 'rev'),
                Query::sum('rev.score', 'total'),
                Query::count('*', 'cnt'),
                Query::groupBy(['name']),
                Query::having([Query::equal('total', [1100])]),
            ]));
            $this->assertSame(0, \count($havingSum));
            $this->assertComboSecretsHidden($havingSum);

            $havingCount = $database->skipValidation(fn () => $database->find($mCol, [
                Query::join($secCol, '$id', 'mainId', '=', 'rev'),
                Query::sum('rev.score', 'total'),
                Query::count('*', 'cnt'),
                Query::groupBy(['name']),
                Query::having([Query::equal('cnt', [3])]),
            ]));
            $this->assertSame(0, \count($havingCount));
            $this->assertComboSecretsHidden($havingCount);
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboCursorAfterJoinSideOrderAttribute(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $pubCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $pubCol): void {
            $full = $database->skipValidation(fn () => $database->find($mCol, [
                Query::join($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::orderAsc('pub.score'),
            ]));
            $this->assertSame(2, \count($full));
            $this->assertComboSecretsHidden($full);
            $this->assertSame([10, 313], $this->comboNumericScores($full));

            $first = $database->skipValidation(fn () => $database->find($mCol, [
                Query::join($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::orderAsc('pub.score'),
                Query::limit(1),
            ]));
            $this->assertSame(1, \count($first));
            $this->assertSame(10, $this->comboNumericScores($first)[0]);
            $this->assertComboSecretsHidden($first);

            $next = $database->skipValidation(fn () => $database->find($mCol, [
                Query::join($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::orderAsc('pub.score'),
                Query::cursorAfter($first[0]),
                Query::limit(1),
            ]));
            $this->assertSame(1, \count($next));
            $this->assertComboSecretsHidden($next);
            $this->assertSame(313, $this->comboNumericScores($next)[0]);
            $this->assertSame($full[1]->getId(), $next[0]->getId());
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboFullOuterPlusLeftThenIsNull(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $pubCol, $secCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $pubCol, $secCol): void {
            $results = $database->skipValidation(fn () => $database->find($mCol, [
                Query::fullOuterJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::leftJoin($secCol, '$id', 'mainId', '=', 'sec'),
                Query::isNull('sec.score'),
                Query::select(['name', 'pub.score']),
            ]));

            $this->assertGreaterThanOrEqual(1, \count($results));
            $this->assertComboSecretsHidden($results);

            $scores = $this->comboNumericScores($results);
            $this->assertContains(4242, $scores);
            $this->assertSame(false, \in_array(777, $scores, true));
            $this->assertSame(false, \in_array(313, $scores, true));
            $this->assertSame(false, \in_array(10, $scores, true));

            foreach ($results as $document) {
                $this->assertNotSame('j-combo-secret', $document->getId());
                $pubScore = $document->getAttribute('pub.score') ?? $document->getAttribute('score');
                if (\is_numeric($pubScore) && (int) $pubScore === 4242) {
                    continue;
                }
                $secScore = $document->getAttribute('sec.score');
                $this->assertTrue($secScore === null || $secScore === '');
            }
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboCrossJoinEqualDoesNotCartesianExplodeSecrets(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, , $secCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $secCol): void {
            $crossed = $database->find($mCol, [
                Query::crossJoin($secCol, 'sec'),
            ]);
            $this->assertSame(6, \count($crossed));
            $this->assertComboSecretsHidden($crossed);
            $this->assertSame(false, \in_array(777, $this->comboNumericScores($crossed), true));

            $equal = $database->skipValidation(fn () => $database->find($mCol, [
                Query::crossJoin($secCol, 'sec'),
                Query::equal('sec.score', [10]),
            ]));
            $this->assertSame(2, \count($equal));
            $this->assertComboSecretsHidden($equal);
            $this->assertSame([10, 10], $this->comboNumericScores($equal));
            $this->assertSame(false, \in_array(777, $this->comboNumericScores($equal), true));
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboGetDocumentLeftUnmatchedAndInnerMatched(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $pubCol, $secCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $pubCol, $secCol): void {
            $unmatched = $database->getDocument($mCol, 'm2', [
                Query::leftJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::join($secCol, '$id', 'mainId', '=', 'sec'),
            ]);
            $this->assertSame(false, $unmatched->isEmpty());
            $this->assertSame('m2', $unmatched->getId());
            $this->assertComboSecretHidden($unmatched);
            $this->assertSame(313, (int) $unmatched->getAttribute('score'));

            $matched = $database->getDocument($mCol, 'm1', [
                Query::leftJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::join($secCol, '$id', 'mainId', '=', 'sec'),
            ]);
            $this->assertSame(false, $matched->isEmpty());
            $this->assertSame('m1', $matched->getId());
            $this->assertComboSecretHidden($matched);
            $this->assertNotSame(777, $matched->getAttribute('score'));
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboSelectJoinPermissionsAndIdDoesNotLeak(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, , $secCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $secCol): void {
            $results = $database->find($mCol, [
                Query::leftJoin($secCol, '$id', 'mainId', '=', 'sec'),
                Query::select(['name', 'sec.$id', 'sec.$permissions']),
            ]);

            $this->assertGreaterThanOrEqual(1, \count($results));
            $this->assertComboSecretsHidden($results);

            foreach ($results as $document) {
                $this->assertNotSame('j-combo-secret', $document->getId());
                $this->assertContains($document->getId(), ['m1', 'm2']);
                foreach ($document->getPermissions() as $permission) {
                    $this->assertSame(false, \str_contains($permission, 'user:combo-hidden'));
                    $this->assertSame(false, \str_contains($permission, 'combo-secret-perm'));
                    $this->assertSame(false, \str_contains($permission, 'j-combo-secret'));
                }
            }
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboChainDocSecOffThenOnHidesSecretC(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, , , $selfCol, $cCol] = $this->seedJoinComboFixture($database);

        $any = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->updateCollection($selfCol, $any, false);
        $database->updateCollection($cCol, $any, false);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $selfCol, $cCol, $any): void {
            $visible = $database->find($mCol, [
                Query::join($selfCol, '$id', 'mainId', '=', 'mid'),
                Query::join($cCol, 'mid.$id', 'selfId', '=', 'c'),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($visible));
            $visibleEncoded = \json_encode(\array_map(static fn (Document $document): array => $document->getArrayCopy(), $visible));
            $this->assertNotFalse($visibleEncoded);
            $this->assertSame(true, \str_contains($visibleEncoded, 'c-combo-secret'));

            $database->updateCollection($cCol, $any, true);

            $hidden = $database->find($mCol, [
                Query::join($selfCol, '$id', 'mainId', '=', 'mid'),
                Query::join($cCol, 'mid.$id', 'selfId', '=', 'c'),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($hidden));
            $this->assertComboSecretsHidden($hidden);
            foreach ($hidden as $document) {
                $this->assertNotSame('c-combo-secret', $document->getId());
                $this->assertNotSame('c-combo-secret', $document->getAttribute('secret'));
                $this->assertNotSame('c-combo-secret', $document->getAttribute('selfId'));
            }
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinComboDottedAttributeNameDoesNotSplitAsAlias(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $pubCol] = $this->seedJoinComboFixture($database);
        $database->createAttribute($mCol, new Attribute(key: 'rev.score', type: ColumnType::Integer, size: 0, required: false));
        $database->getAuthorization()->skip(function () use ($database, $mCol): void {
            $database->deleteDocument($mCol, 'm1');
            $database->deleteDocument($mCol, 'm2');
            $database->createDocument($mCol, new Document([
                '$id' => 'm1',
                'name' => 'Main',
                'rev.score' => 21,
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($mCol, new Document([
                '$id' => 'm2',
                'name' => 'Unmatched',
                'rev.score' => 22,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        });

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $pubCol): void {
            $selected = $database->find($mCol, [
                Query::leftJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::select(['name', 'rev.score']),
                Query::orderDesc('rev.score'),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($selected));
            $this->assertComboSecretsHidden($selected);

            $dotted = [];
            foreach ($selected as $document) {
                $value = $document->getAttribute('rev.score') ?? $document->getAttribute('revscore');
                if (\is_numeric($value)) {
                    $dotted[] = (int) $value;
                }
            }
            $this->assertContains(21, $dotted);
            $this->assertContains(22, $dotted);
            $this->assertSame(false, \in_array(313, $dotted, true));
            $this->assertSame(false, \in_array(777, $dotted, true));

            $filtered = $database->find($mCol, [
                Query::leftJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::equal('rev.score', [21]),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($filtered));
            $this->assertComboSecretsHidden($filtered);
            foreach ($filtered as $document) {
                $value = $document->getAttribute('rev.score') ?? $document->getAttribute('revscore');
                $this->assertSame(21, (int) $value);
            }
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    /**
     * @return list<string>
     */
    private function joinComboCollections(): array
    {
        return ['jc_m', 'jc_pub', 'jc_sec', 'jc_self', 'jc_c'];
    }

    /**
     * @return array{0: string, 1: string, 2: string, 3: string, 4: string}
     */
    private function seedJoinComboFixture(Database $database): array
    {
        $mCol = 'jc_m';
        $pubCol = 'jc_pub';
        $secCol = 'jc_sec';
        $selfCol = 'jc_self';
        $cCol = 'jc_c';
        $this->cleanupAggCollections($database, [$mCol, $pubCol, $secCol, $selfCol, $cCol]);

        $any = [Permission::create(Role::any()), Permission::read(Role::any())];

        $database->createCollection($mCol, permissions: $any, documentSecurity: false);
        $database->createAttribute($mCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($pubCol, permissions: $any, documentSecurity: true);
        $database->createAttribute($pubCol, new Attribute(key: 'mainId', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($pubCol, new Attribute(key: 'score', type: ColumnType::Integer, size: 0, required: true));

        $database->createCollection($secCol, permissions: $any, documentSecurity: true);
        $database->createAttribute($secCol, new Attribute(key: 'mainId', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($secCol, new Attribute(key: 'score', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute($secCol, new Attribute(key: 'secret', type: ColumnType::String, size: 100, required: false));

        $database->createCollection($selfCol, permissions: $any, documentSecurity: true);
        $database->createAttribute($selfCol, new Attribute(key: 'payload', type: ColumnType::String, size: 100, required: true));
        $database->createAttribute($selfCol, new Attribute(key: 'tag', type: ColumnType::String, size: 50, required: true));
        $database->createAttribute($selfCol, new Attribute(key: 'mainId', type: ColumnType::String, size: 255, required: false));

        $database->createCollection($cCol, permissions: $any, documentSecurity: true);
        $database->createAttribute($cCol, new Attribute(key: 'selfId', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($cCol, new Attribute(key: 'secret', type: ColumnType::String, size: 100, required: true));

        $readAny = [Permission::read(Role::any())];
        $hidden = [
            Permission::read(Role::user('combo-hidden')),
            Permission::update(Role::user('combo-secret-perm')),
        ];

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'm2',
            'name' => 'Unmatched',
            '$permissions' => $readAny,
        ]));

        $database->createDocument($pubCol, new Document([
            '$id' => 'j-pub-auth',
            'mainId' => 'm1',
            'score' => 313,
            '$permissions' => $readAny,
        ]));
        $database->createDocument($pubCol, new Document([
            '$id' => 'j-pub-sib',
            'mainId' => 'm1',
            'score' => 10,
            '$permissions' => $readAny,
        ]));
        $database->createDocument($pubCol, new Document([
            '$id' => 'j-pub-orphan',
            'mainId' => 'missing',
            'score' => 4242,
            '$permissions' => $readAny,
        ]));

        $database->createDocument($secCol, new Document([
            '$id' => 'j-combo-auth',
            'mainId' => 'm1',
            'score' => 313,
            'secret' => 'visible',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($secCol, new Document([
            '$id' => 'j-combo-ten',
            'mainId' => 'm1',
            'score' => 10,
            'secret' => 'visible-ten',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($secCol, new Document([
            '$id' => 'j-combo-secret',
            'mainId' => 'm1',
            'score' => 777,
            'secret' => 'combo-secret-alpha',
            '$permissions' => $hidden,
        ]));
        $database->createDocument($secCol, new Document([
            '$id' => 'j-combo-m2',
            'mainId' => 'm2',
            'score' => 313,
            'secret' => 'visible-m2',
            '$permissions' => $readAny,
        ]));

        $database->createDocument($selfCol, new Document([
            '$id' => 'open',
            'payload' => 'open-payload',
            'tag' => 'shared',
            'mainId' => 'm1',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($selfCol, new Document([
            '$id' => 'hidden-self',
            'payload' => 'combo-secret-alpha',
            'tag' => 'shared',
            'mainId' => 'm1',
            '$permissions' => $hidden,
        ]));

        $database->createDocument($cCol, new Document([
            '$id' => 'c-open',
            'selfId' => 'open',
            'secret' => 'c-open-token',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($cCol, new Document([
            '$id' => 'c-combo-secret',
            'selfId' => 'open',
            'secret' => 'c-combo-secret',
            '$permissions' => $hidden,
        ]));

        return [$mCol, $pubCol, $secCol, $selfCol, $cCol];
    }

    /**
     * @param list<string> $roles
     * @param callable(): void $callback
     */
    private function withComboRoles(Database $database, array $roles, callable $callback): void
    {
        $authorization = $database->getAuthorization();
        $previousRoles = $authorization->getRoles();
        $authorization->cleanRoles();
        foreach ($roles as $role) {
            $authorization->addRole($role);
        }

        try {
            $callback();
        } finally {
            $authorization->cleanRoles();
            foreach ($previousRoles as $role) {
                $authorization->addRole($role);
            }
        }
    }

    /**
     * @param array<Document> $documents
     */
    private function assertComboSecretsHidden(array $documents): void
    {
        $payload = [];
        foreach ($documents as $document) {
            $this->assertComboSecretHidden($document);
            $payload[] = $document->getArrayCopy();
        }

        $this->assertEncodedComboSecretHidden(\json_encode($payload));
    }

    private function assertComboSecretHidden(Document $document): void
    {
        $this->assertNotSame('j-combo-secret', $document->getId());
        $this->assertNotSame('c-combo-secret', $document->getId());

        $score = $document->getAttribute('score');
        if (\is_numeric($score)) {
            $this->assertNotSame(777, (int) $score);
        } else {
            $this->assertNotSame(777, $score);
        }

        $this->assertNotSame('combo-secret-alpha', $document->getAttribute('secret'));
        $this->assertNotSame('combo-secret-alpha', $document->getAttribute('payload'));

        foreach ($document->getPermissions() as $permission) {
            $this->assertSame(false, \str_contains($permission, 'j-combo-secret'));
            $this->assertSame(false, \str_contains($permission, 'c-combo-secret'));
            $this->assertSame(false, \str_contains($permission, 'user:combo-hidden'));
            $this->assertSame(false, \str_contains($permission, 'combo-secret-perm'));
        }

        $this->assertEncodedComboSecretHidden(\json_encode($document));
    }

    private function assertEncodedComboSecretHidden(string|false $encoded): void
    {
        $this->assertNotFalse($encoded);
        $this->assertSame(false, \str_contains($encoded, 'j-combo-secret'));
        $this->assertSame(false, \str_contains($encoded, 'c-combo-secret'));
        $this->assertSame(false, \str_contains($encoded, 'user:combo-hidden'));
        $this->assertSame(false, \str_contains($encoded, 'combo-secret-perm'));
        $this->assertSame(false, \str_contains($encoded, 'combo-secret-alpha'));
        $this->assertSame(false, $this->comboEncodedJsonContainsScalar($encoded, 777));
    }

    private function comboEncodedJsonContainsScalar(string $encoded, int $needle): bool
    {
        $decoded = \json_decode($encoded, true);
        if (! \is_array($decoded)) {
            return false;
        }

        return $this->comboJsonContainsScalar($decoded, $needle);
    }

    private function comboJsonContainsScalar(mixed $value, int $needle, string|int|null $key = null): bool
    {
        if (\is_int($value) || \is_float($value) || (\is_string($value) && \is_numeric($value))) {
            if ($this->isIgnoredJoinComboSecretKey($key)) {
                return false;
            }

            return (int) $value === $needle;
        }

        if (! \is_array($value)) {
            return false;
        }

        foreach ($value as $childKey => $child) {
            if ($this->comboJsonContainsScalar($child, $needle, $childKey)) {
                return true;
            }
        }

        return false;
    }

    private function isIgnoredJoinComboSecretKey(string|int|null $key): bool
    {
        return \in_array($key, [
            Document::SEQUENCE,
            Document::CREATED_AT,
            Document::UPDATED_AT,
            Document::TENANT,
            Document::VERSION,
            Document::COLLECTION,
            Document::DISTANCE,
            Document::DELETED_AT,
            Document::INTERNAL_ID,
            Document::SKIP_PERMISSIONS_UPDATE,
        ], true);
    }

    /**
     * @param array<Document> $documents
     * @return list<int>
     */
    private function comboNumericScores(array $documents): array
    {
        $scores = [];
        foreach ($documents as $document) {
            $score = $document->getAttribute('pub.score')
                ?? $document->getAttribute('sec.score')
                ?? $document->getAttribute('rev.score')
                ?? $document->getAttribute('score');
            if (\is_numeric($score)) {
                $scores[] = (int) $score;
            }
        }

        return $scores;
    }

    /**
     * @param list<int> $scores
     * @return list<int>
     */
    private function sortedDesc(array $scores): array
    {
        $sorted = $scores;
        \rsort($sorted, SORT_NUMERIC);

        return $sorted;
    }
}
