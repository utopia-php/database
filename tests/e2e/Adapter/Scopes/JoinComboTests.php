<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
use Utopia\Query\Schema\IndexType;

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
            $ordered = $database->find($mCol, [
                Query::leftJoin($secCol, '$id', 'mainId', '=', 'sec'),
                Query::orderDesc('sec.score'),
            ]);
            $this->assertComboSecretsHidden($ordered);
            $scores = $this->comboNumericScores($ordered);
            $this->assertSame(false, \in_array(777, $scores, true));
            $this->assertContains(313, $scores);
            $this->assertSame($scores, $this->sortedDesc($scores));

            $limited = $database->find($mCol, [
                Query::leftJoin($secCol, '$id', 'mainId', '=', 'sec'),
                Query::orderDesc('sec.score'),
                Query::limit(2),
            ]);
            $this->assertSame(2, \count($limited));
            $this->assertComboSecretsHidden($limited);
            $this->assertSame(
                \array_slice($this->comboNumericScores($ordered), 0, 2),
                $this->comboNumericScores($limited)
            );

            $offset = $database->find($mCol, [
                Query::leftJoin($secCol, '$id', 'mainId', '=', 'sec'),
                Query::orderDesc('sec.score'),
                Query::limit(2),
                Query::offset(1),
            ]);
            $this->assertSame(2, \count($offset));
            $this->assertComboSecretsHidden($offset);
            $this->assertSame(
                \array_slice($this->comboNumericScores($ordered), 1, 2),
                $this->comboNumericScores($offset)
            );

            $foj = $database->find($mCol, [
                Query::fullOuterJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::orderDesc('pub.score'),
            ]);
            $this->assertComboSecretsHidden($foj);
            $fojScores = $this->comboNumericScores($foj);
            $this->assertSame(4242, $fojScores[0]);
            $this->assertContains(313, $fojScores);
            $this->assertSame(false, \in_array(777, $fojScores, true));
            $this->assertSame($fojScores, $this->sortedDesc($fojScores));

            $fojLimited = $database->find($mCol, [
                Query::fullOuterJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::orderDesc('pub.score'),
                Query::limit(2),
                Query::offset(1),
            ]);
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
            $results = $database->find($mCol, [
                Query::join($secCol, '$id', 'mainId', '=', 'rev'),
                Query::equal('rev.score', [777]),
                Query::select(['name']),
            ]);

            $this->assertSame(0, \count($results));
            $this->assertComboSecretsHidden($results);
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
            $aggregated = $database->find($mCol, [
                Query::join($secCol, '$id', 'mainId', '=', 'rev'),
                Query::equal('name', ['Main']),
                Query::sum('rev.score', 'total'),
                Query::count('*', 'cnt'),
                Query::groupBy(['name']),
            ]);

            $this->assertSame(1, \count($aggregated));
            $this->assertComboSecretsHidden($aggregated);
            $total = $aggregated[0]->getAttribute('total');
            $this->assertTrue(\is_numeric($total));
            $this->assertSame(323, (int) $total);
            $this->assertNotSame(1100, (int) $total);
            $cnt = $aggregated[0]->getAttribute('cnt');
            $this->assertTrue(\is_numeric($cnt));
            $this->assertSame(2, (int) $cnt);
            $this->assertNotSame(3, (int) $cnt);

            $havingSum = $database->find($mCol, [
                Query::join($secCol, '$id', 'mainId', '=', 'rev'),
                Query::sum('rev.score', 'total'),
                Query::count('*', 'cnt'),
                Query::groupBy(['name']),
                Query::having([Query::equal('total', [1100])]),
            ]);
            $this->assertSame(0, \count($havingSum));
            $this->assertComboSecretsHidden($havingSum);

            $havingCount = $database->find($mCol, [
                Query::join($secCol, '$id', 'mainId', '=', 'rev'),
                Query::sum('rev.score', 'total'),
                Query::count('*', 'cnt'),
                Query::groupBy(['name']),
                Query::having([Query::equal('cnt', [3])]),
            ]);
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
            $full = $database->find($mCol, [
                Query::join($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::orderAsc('pub.score'),
            ]);
            $this->assertSame(2, \count($full));
            $this->assertComboSecretsHidden($full);
            $this->assertSame([10, 313], $this->comboNumericScores($full));

            $first = $database->find($mCol, [
                Query::join($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::orderAsc('pub.score'),
                Query::limit(1),
            ]);
            $this->assertSame(1, \count($first));
            $this->assertSame(10, $this->comboNumericScores($first)[0]);
            $this->assertComboSecretsHidden($first);

            $next = $database->find($mCol, [
                Query::join($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::orderAsc('pub.score'),
                Query::cursorAfter($first[0]),
                Query::limit(1),
            ]);
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
            $results = $database->find($mCol, [
                Query::fullOuterJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::leftJoin($secCol, '$id', 'mainId', '=', 'sec'),
                Query::isNull('sec.score'),
                Query::select(['name', 'pub.score']),
            ]);

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

            $equal = $database->find($mCol, [
                Query::crossJoin($secCol, 'sec'),
                Query::equal('sec.score', [10]),
            ]);
            $this->assertSame(2, \count($equal));
            $this->assertComboSecretsHidden($equal);
            foreach ($this->comboNumericScores($equal) as $score) {
                $this->assertNotSame(777, $score);
                $this->assertSame(10, $score);
            }
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
            $unmatchedScore = $unmatched->getAttribute('score');
            $this->assertTrue(\is_numeric($unmatchedScore));
            $this->assertSame(313, (int) $unmatchedScore);

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
            $visibleEncoded = \json_encode(\array_map(static function (Document $document): array {
                /** @var array<string, mixed> $copy */
                $copy = $document->getArrayCopy();

                return $copy;
            }, $visible));
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

    public function testJoinComboCountMatchesFindWhenFilteringJoinAlias(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $pubCol, $secCol] = $this->seedJoinComboFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $pubCol, $secCol): void {
            $visible = [
                Query::leftJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::join($secCol, '$id', 'mainId', '=', 'sec'),
                Query::equal('sec.score', [313]),
            ];
            $found = $database->find($mCol, $visible);
            $this->assertSame(\count($found), $database->count($mCol, $visible));
            $this->assertGreaterThan(0, \count($found));
            $this->assertComboSecretsHidden($found);
            $this->assertContains(313, $this->comboNumericScores($found));

            $hiddenScore = [
                Query::leftJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::join($secCol, '$id', 'mainId', '=', 'sec'),
                Query::equal('sec.score', [777]),
            ];
            $hiddenScoreFound = $database->find($mCol, $hiddenScore);
            $this->assertSame(0, \count($hiddenScoreFound));
            $this->assertSame(0, $database->count($mCol, $hiddenScore));
            $this->assertComboSecretsHidden($hiddenScoreFound);

            $hiddenSecret = [
                Query::leftJoin($pubCol, '$id', 'mainId', '=', 'pub'),
                Query::join($secCol, '$id', 'mainId', '=', 'sec'),
                Query::equal('sec.secret', ['combo-secret-alpha']),
            ];
            $hiddenSecretFound = $database->find($mCol, $hiddenSecret);
            $this->assertSame(0, \count($hiddenSecretFound));
            $this->assertSame(0, $database->count($mCol, $hiddenSecret));
            $this->assertComboSecretsHidden($hiddenSecretFound);
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
        $database->createAttribute($mCol, Attribute::integer(key: 'rev.score'));
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
                $this->assertTrue(\is_numeric($value));
                $this->assertSame(21, (int) $value);
            }
        });

        $this->cleanupAggCollections($database, $this->joinComboCollections());
    }

    public function testJoinHardcoreNestedJsonAndJoinAliasSameQuery(): void
    {
        $database = static::getDatabase();
        if (
            ! $database->getAdapter()->supports(Capability::Joins)
            || ! $database->getAdapter()->supports(Capability::Objects)
        ) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $database->createAttribute($mCol, Attribute::object(key: 'profile'));
        if ($database->getAdapter()->supports(Capability::ObjectIndexes)) {
            $database->createIndex($mCol, Index::key(key: 'idx_jh_profile_email', attributes: ['profile.user.email']));
        }

        $database->getAuthorization()->skip(function () use ($database, $mCol): void {
            $main = $database->getDocument($mCol, 'hm1');
            $main->setAttribute('profile', [
                'user' => [
                    'email' => 'alice@hard.example',
                ],
            ]);
            $database->updateDocument($mCol, 'hm1', $main);

            $second = $database->getDocument($mCol, 'hm2');
            $second->setAttribute('profile', [
                'user' => [
                    'email' => 'bob@hard.example',
                ],
            ]);
            $database->updateDocument($mCol, 'hm2', $second);
        });

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $results = $database->find($mCol, [
                Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::select(['name', 'profile', 'meta.score']),
                Query::equal('profile.user.email', ['alice@hard.example']),
                Query::equal('meta.score', [10]),
                Query::orderDesc('meta.score'),
            ]);

            $this->assertSame(1, \count($results));
            $this->assertComboSecretsHidden($results);
            $this->assertSame('hm1', $results[0]->getId());
            $this->assertSame('Main', $results[0]->getAttribute('name'));

            $profile = $results[0]->getAttribute('profile');
            $this->assertTrue(\is_array($profile));
            $user = $profile['user'] ?? null;
            $this->assertTrue(\is_array($user));
            $this->assertSame('alice@hard.example', $user['email'] ?? null);

            $score = $results[0]->getAttribute('meta.score') ?? $results[0]->getAttribute('score');
            $this->assertTrue(\is_numeric($score));
            $this->assertSame(10, (int) $score);
            $this->assertSame(false, \in_array(8686, $this->comboNumericScores($results), true));
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreSameTableTwoAliasesIndependentPredicates(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, , $peerCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $peerCol): void {
            $results = $database->find($mCol, [
                Query::join($peerCol, '$id', 'mainId', '=', 'alpha'),
                Query::join($peerCol, 'peerKey', '$id', '=', 'beta'),
                Query::equal('alpha.label', ['alpha-one']),
                Query::equal('beta.label', ['beta-key']),
                Query::select(['name', 'alpha.$id', 'beta.$id', 'alpha.label', 'beta.label', 'alpha.score']),
                Query::orderDesc('alpha.score'),
            ]);

            $this->assertSame(1, \count($results));
            $this->assertComboSecretsHidden($results);
            $this->assertSame('hm1', $results[0]->getId());
            $this->assertSame('Main', $results[0]->getAttribute('name'));
            $this->assertSame('peer-a', $results[0]->getAttribute('alpha.$id'));
            $this->assertSame('peer-b', $results[0]->getAttribute('beta.$id'));
            $this->assertSame('alpha-one', $results[0]->getAttribute('alpha.label'));
            $this->assertSame('beta-key', $results[0]->getAttribute('beta.label'));
            $this->assertNotSame('peer-a', $results[0]->getId());
            $this->assertNotSame('peer-b', $results[0]->getId());
            $this->assertNotSame('peer-hidden', $results[0]->getId());
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreSelfJoinOnIdDoesNotSmashIdentity(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol): void {
            $results = $database->find($mCol, [
                Query::join($mCol, '$id', '$id', '=', 'twin'),
                Query::select(['name', 'rank', 'twin.$id', 'twin.name', 'twin.$permissions']),
                Query::orderAsc('rank'),
            ]);

            $this->assertSame(3, \count($results));
            $this->assertComboSecretsHidden($results);

            $names = [
                'hm1' => 'Main',
                'hm2' => 'Second',
                'hm3' => 'Third',
            ];
            $ids = [];
            foreach ($results as $document) {
                $id = $document->getId();
                $ids[] = $id;
                $this->assertArrayHasKey($id, $names);
                $this->assertSame($names[$id], $document->getAttribute('name'));
                $this->assertSame($id, $document->getAttribute('twin.$id'));
                $this->assertSame($names[$id], $document->getAttribute('twin.name'));
            }
            \sort($ids);
            $this->assertSame(['hm1', 'hm2', 'hm3'], $ids);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreLeftInnerRightMixedDocSec(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol, , , $bCol, $cCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol, $bCol, $cCol): void {
            $results = $database->find($mCol, [
                Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::join($bCol, '$id', 'mainId', '=', 'mid'),
                Query::rightJoin($cCol, '$id', 'mainId', '=', 'tail'),
                Query::select(['name', 'meta.score', 'mid.label', 'tail.secret', 'tail.score']),
            ]);

            $this->assertGreaterThanOrEqual(1, \count($results));
            $this->assertComboSecretsHidden($results);

            $ids = \array_map(static fn (Document $document): string => $document->getId(), $results);
            $this->assertContains('hm1', $ids);
            if (! $database->getSharedTables()) {
                $this->assertContains('', $ids);
            }
            $this->assertSame(false, \in_array('hm2', $ids, true));
            $this->assertSame(false, \in_array('hm3', $ids, true));
            $this->assertSame(false, \in_array('hc-hidden', $ids, true));
            $this->assertSame(false, \in_array('hc-right', $ids, true));

            $labels = [];
            foreach ($results as $document) {
                $label = $document->getAttribute('mid.label') ?? $document->getAttribute('label');
                if (\is_string($label) && $label !== '') {
                    $labels[] = $label;
                }
                $this->assertNotSame('combo-hard-alpha', $document->getAttribute('tail.secret'));
                $this->assertNotSame('combo-hard-alpha', $document->getAttribute('secret'));
            }
            $this->assertContains('b-public', $labels);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreChainAOnBOffCOnHidesC(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [, , , $aCol, $bCol, $cCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $aCol, $bCol, $cCol): void {
            $results = $database->find($aCol, [
                Query::distinct(),
                Query::join($bCol, '$id', 'aId', '=', 'b'),
                Query::join($cCol, 'b.$id', 'bId', '=', 'c'),
                Query::select(['$id', 'name', 'b.label', 'c.secret', 'c.score']),
            ]);

            $this->assertGreaterThanOrEqual(1, \count($results));
            $this->assertComboSecretsHidden($results);

            $labels = [];
            foreach ($results as $document) {
                $this->assertSame('ha1', $document->getId());
                $label = $document->getAttribute('b.label') ?? $document->getAttribute('label');
                if (\is_string($label) && $label !== '') {
                    $labels[] = $label;
                }
                $this->assertNotSame('hc-hidden', $document->getId());
                $this->assertNotSame('combo-hard-alpha', $document->getAttribute('c.secret'));
                $this->assertNotSame('combo-hard-alpha', $document->getAttribute('secret'));
                $score = $document->getAttribute('c.score') ?? $document->getAttribute('score');
                if (\is_numeric($score)) {
                    $this->assertNotSame(8686, (int) $score);
                }
            }
            $this->assertContains('b-public', $labels);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreFullOuterJoinSideCursorPageWalk(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $ordered = [
                Query::fullOuterJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::orderAsc('meta.score'),
            ];

            $full = $database->find($mCol, $ordered);
            $this->assertGreaterThanOrEqual(2, \count($full));
            $this->assertComboSecretsHidden($full);

            $fullScores = $this->comboNumericScores($full);
            $this->assertContains(10, $fullScores);
            $this->assertContains(15, $fullScores);
            $this->assertContains(20, $fullScores);
            $this->assertContains(42, $fullScores);
            $this->assertContains(313, $fullScores);
            $this->assertSame(false, \in_array(8686, $fullScores, true));
            $this->assertSame($fullScores, $this->sortedAsc($fullScores));

            $cursor = null;
            foreach ($full as $document) {
                $score = $this->comboJoinScore($document);
                if ($document->getId() !== '' && $score !== null) {
                    $cursor = $document;
                    break;
                }
            }
            $this->assertNotNull($cursor);
            $this->assertNotSame('', $cursor->getId());
            $cursorScore = $this->comboJoinScore($cursor);
            $this->assertNotNull($cursorScore);

            $after = $database->find($mCol, [
                ...$ordered,
                Query::cursorAfter($cursor),
                Query::limit(1),
            ]);
            $this->assertSame(1, \count($after));
            $this->assertComboSecretsHidden($after);
            $afterScore = $this->comboJoinScore($after[0]);
            $this->assertNotNull($afterScore);

            $cursorIndex = \array_search($cursorScore, $fullScores, true);
            $this->assertNotSame(false, $cursorIndex);
            $nextIndex = (int) $cursorIndex + 1;
            $this->assertArrayHasKey($nextIndex, $fullScores);
            $this->assertSame($fullScores[$nextIndex], $afterScore);

            $beforeCursor = null;
            for ($index = \count($full) - 1; $index >= 0; $index--) {
                $score = $this->comboJoinScore($full[$index]);
                if ($full[$index]->getId() !== '' && $score !== null) {
                    $beforeCursor = $full[$index];
                    break;
                }
            }
            $this->assertNotNull($beforeCursor);
            $this->assertNotSame('', $beforeCursor->getId());
            $beforeCursorScore = $this->comboJoinScore($beforeCursor);
            $this->assertNotNull($beforeCursorScore);

            $before = $database->find($mCol, [
                ...$ordered,
                Query::cursorBefore($beforeCursor),
                Query::limit(1),
            ]);
            $this->assertSame(1, \count($before));
            $this->assertComboSecretsHidden($before);
            $beforeScore = $this->comboJoinScore($before[0]);
            $this->assertNotNull($beforeScore);

            $beforeIndex = \array_search($beforeCursorScore, $fullScores, true);
            $this->assertNotSame(false, $beforeIndex);
            $previousIndex = (int) $beforeIndex - 1;
            $this->assertArrayHasKey($previousIndex, $fullScores);
            $this->assertSame($fullScores[$previousIndex], $beforeScore);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreAndOrMixMainAndJoinFilters(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $mixed = $database->find($mCol, [
                Query::join($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::and([
                    Query::equal('name', ['Main']),
                    Query::or([
                        Query::equal('meta.score', [10]),
                        Query::equal('rank', [2]),
                    ]),
                ]),
                Query::select(['name', 'meta.score']),
            ]);

            $this->assertSame(1, \count($mixed));
            $this->assertComboSecretsHidden($mixed);
            $this->assertSame('hm1', $mixed[0]->getId());
            $score = $mixed[0]->getAttribute('meta.score') ?? $mixed[0]->getAttribute('score');
            $this->assertTrue(\is_numeric($score));
            $this->assertSame(10, (int) $score);

            $hiddenOnly = $database->find($mCol, [
                Query::join($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::and([
                    Query::equal('name', ['Main']),
                    Query::or([
                        Query::equal('meta.score', [8686]),
                        Query::equal('meta.secret', ['combo-hard-alpha']),
                    ]),
                ]),
            ]);
            $this->assertSame(0, \count($hiddenOnly));
            $this->assertComboSecretsHidden($hiddenOnly);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreMixedMainJoinOrderCursor(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $ordered = [
                Query::join($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::orderAsc('rank'),
                Query::orderDesc('meta.score'),
            ];

            $full = $database->find($mCol, $ordered);
            $this->assertSame(4, \count($full));
            $this->assertComboSecretsHidden($full);
            $this->assertSame([313, 15, 10, 20], $this->comboNumericScores($full));

            $first = $database->find($mCol, [
                ...$ordered,
                Query::limit(1),
            ]);
            $this->assertSame(1, \count($first));
            $this->assertComboSecretsHidden($first);
            $this->assertSame('hm1', $first[0]->getId());
            $this->assertSame(313, $this->comboNumericScores($first)[0]);

            $next = $database->find($mCol, [
                ...$ordered,
                Query::cursorAfter($first[0]),
                Query::limit(1),
            ]);
            $this->assertSame(1, \count($next));
            $this->assertComboSecretsHidden($next);
            $this->assertSame($full[1]->getId(), $next[0]->getId());
            $this->assertSame(15, $this->comboNumericScores($next)[0]);

            $before = $database->find($mCol, [
                ...$ordered,
                Query::cursorBefore($next[0]),
                Query::limit(1),
            ]);
            $this->assertSame(1, \count($before));
            $this->assertComboSecretsHidden($before);
            $this->assertSame($first[0]->getId(), $before[0]->getId());
            $this->assertSame(313, $this->comboNumericScores($before)[0]);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreJoinSideOperatorsAndInternalAttrs(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $results = $database->find($mCol, [
                Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::containsString('meta.body', ['hard-needle']),
                Query::between('meta.score', 1, 50),
                Query::startsWith('meta.label', 'visible'),
                Query::equal('meta.$id', ['hm-meta-10']),
                Query::greaterThan('meta.$createdAt', '2000-01-01 00:00:00.000'),
                Query::select(['name', 'meta.$id', 'meta.score', 'meta.label', 'meta.body']),
            ]);

            $this->assertSame(1, \count($results));
            $this->assertComboSecretsHidden($results);
            $this->assertSame('hm1', $results[0]->getId());
            $this->assertSame('hm-meta-10', $results[0]->getAttribute('meta.$id'));
            $score = $results[0]->getAttribute('meta.score') ?? $results[0]->getAttribute('score');
            $this->assertTrue(\is_numeric($score));
            $this->assertSame(10, (int) $score);

            if (
                $database->getAdapter()->supports(Capability::Fulltext)
                && $this->joinHardcoreHasFulltextIndex($database, $metaCol)
            ) {
                $searched = $database->find($mCol, [
                    Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                    Query::search('meta.body', 'needle'),
                ]);
                $this->assertGreaterThanOrEqual(1, \count($searched));
                $this->assertComboSecretsHidden($searched);
                $this->assertSame('hm1', $searched[0]->getId());
            }
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreRightUnmatchedMainIdentityAndSelectSubset(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $results = $database->find($mCol, [
                Query::rightJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::select(['name', 'meta.score', 'meta.secret']),
            ]);

            $this->assertGreaterThanOrEqual(2, \count($results));
            $this->assertComboSecretsHidden($results);

            $unmatched = null;
            $ids = [];
            foreach ($results as $document) {
                $ids[] = $document->getId();
                $this->assertNotSame('hm-meta-orphan', $document->getId());
                $this->assertNotSame('hm-meta-secret', $document->getId());
                $this->assertNotSame('hm-meta-10', $document->getId());
                $this->assertNotSame('combo-hard-alpha', $document->getAttribute('meta.secret'));
                $this->assertNotSame('combo-hard-alpha', $document->getAttribute('secret'));
                if ($document->getId() === '') {
                    $unmatched = $document;
                }
            }

            $this->assertNotNull($unmatched);
            $this->assertSame('', $unmatched->getId());
            $this->assertTrue($unmatched->getAttribute('name') === null || $unmatched->getAttribute('name') === '');
            $orphanScore = $unmatched->getAttribute('meta.score') ?? $unmatched->getAttribute('score');
            $this->assertTrue(\is_numeric($orphanScore));
            $this->assertSame(42, (int) $orphanScore);
            $this->assertSame(false, \in_array('hm-meta-orphan', $ids, true));
            $this->assertContains('hm1', $ids);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreCoerceSecret8686AbsentWhenUnauthorized(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $results = $database->find($mCol, [
                Query::join($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::equal('meta.score', ['8686']),
                Query::select(['name', 'meta.score', 'meta.secret']),
            ]);

            $this->assertSame(0, \count($results));
            $this->assertComboSecretsHidden($results);

            $encoded = \json_encode(\array_map(static function (Document $document): array {
                /** @var array<string, mixed> $copy */
                $copy = $document->getArrayCopy();

                return $copy;
            }, $results));
            $this->assertNotFalse($encoded);
            $this->assertSame(false, $this->comboEncodedJsonContainsScalar($encoded, 8686));
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreSharedTablesSecret5151NotTenant(): void
    {
        $database = static::getDatabase();
        if (
            ! $database->getAdapter()->supports(Capability::Joins)
            || ! $database->getAdapter()->supports(Capability::Schemas)
        ) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $sharedTables = $database->getSharedTables();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();
        $tenant = $database->getTenant();

        $sharedTablesDb = 'sharedTablesJh_'.static::getTestToken();
        $mCol = 'jh_m';
        $metaCol = 'jh_meta';

        try {
            if ($database->exists($sharedTablesDb)) {
                $database->setDatabase($sharedTablesDb)->delete();
            }

            $database
                ->setDatabase($sharedTablesDb)
                ->setNamespace('')
                ->setSharedTables(true)
                ->setTenant(null)
                ->create();

            $any = [Permission::create(Role::any()), Permission::read(Role::any())];
            $database->createCollection(new Collection(id: $mCol, permissions: $any, documentSecurity: false));
            $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));

            $database->createCollection(new Collection(id: $metaCol, permissions: $any));
            $database->createAttribute($metaCol, Attribute::string(key: 'mainId', required: true));
            $database->createAttribute($metaCol, Attribute::integer(key: 'score', required: true));
            $database->createAttribute($metaCol, Attribute::string(key: 'secret', size: 100));

            $database->setTenant(5151);
            $database->createDocument($mCol, new Document([
                '$id' => 'hm1',
                'name' => 'Main',
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($metaCol, new Document([
                '$id' => 'hm-meta-10',
                'mainId' => 'hm1',
                'score' => 10,
                'secret' => 'visible',
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($metaCol, new Document([
                '$id' => 'hm-meta-secret',
                'mainId' => 'hm1',
                'score' => 5151,
                'secret' => 'combo-hard-alpha',
                '$permissions' => [
                    Permission::read(Role::user('combo-hard-hidden')),
                ],
            ]));

            $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
                $results = $database->find($mCol, [
                    Query::leftJoin($metaCol, '$id', 'mainId', '=', 'sec'),
                    Query::select(['name', 'sec.score', 'sec.secret', 'sec.$tenant']),
                ]);

                $this->assertGreaterThanOrEqual(1, \count($results));
                $this->assertComboSecretsHidden($results);

                $payloads = [];
                foreach ($results as $document) {
                    $this->assertSame('hm1', $document->getId());
                    $this->assertNotSame('hm-meta-secret', $document->getId());
                    $score = $document->getAttribute('sec.score') ?? $document->getAttribute('score');
                    if (\is_numeric($score)) {
                        $payloads[] = (int) $score;
                    }
                    $this->assertNotSame('combo-hard-alpha', $document->getAttribute('sec.secret'));
                    $this->assertNotSame('combo-hard-alpha', $document->getAttribute('secret'));
                }
                $this->assertContains(10, $payloads);
                $this->assertSame(false, \in_array(5151, $payloads, true));

                $encoded = \json_encode(\array_map(static function (Document $document): array {
                    /** @var array<string, mixed> $copy */
                    $copy = $document->getArrayCopy();

                    return $copy;
                }, $results));
                $this->assertNotFalse($encoded);
                $this->assertSame(false, $this->comboEncodedJsonContainsScalar($encoded, 5151));
                $this->assertSame(false, \str_contains($encoded, 'combo-hard-alpha'));
            });
        } finally {
            $database->setTenant(null)->setSharedTables(false);
            if ($database->exists($sharedTablesDb)) {
                $database->delete($sharedTablesDb);
            }
            $database
                ->setSharedTables($sharedTables)
                ->setTenant($tenant)
                ->setNamespace($namespace)
                ->setDatabase($schema);
        }
    }

    public function testJoinHardcoreSkipAuthMixedDocSecStillHidesSecrets(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $withoutJoins = $database->find($mCol);
            $this->assertSame(3, \count($withoutJoins));
            $this->assertComboSecretsHidden($withoutJoins);
            $withoutIds = \array_map(static fn (Document $document): string => $document->getId(), $withoutJoins);
            \sort($withoutIds);
            $this->assertSame(['hm1', 'hm2', 'hm3'], $withoutIds);

            $joinedQueries = [
                Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::select(['name', 'meta.score', 'meta.secret']),
            ];
            $joined = $database->find($mCol, $joinedQueries);
            $this->assertSame(4, \count($joined));
            $this->assertComboSecretsHidden($joined);

            $scores = $this->comboNumericScores($joined);
            $this->assertContains(10, $scores);
            $this->assertContains(313, $scores);
            $this->assertContains(20, $scores);
            $this->assertContains(15, $scores);
            $this->assertSame(false, \in_array(8686, $scores, true));
            $this->assertSame(false, \in_array(42, $scores, true));

            $this->assertSame(\count($joined), $database->count($mCol, $joinedQueries));

            $sum = $database->sum($mCol, 'meta.score', [
                Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
            ]);
            $this->assertSame(358, (int) $sum);

            $document = $database->getDocument($mCol, 'hm1', $joinedQueries);
            $this->assertSame(false, $document->isEmpty());
            $this->assertSame('hm1', $document->getId());
            $this->assertComboSecretsHidden([$document]);
            $documentScore = $document->getAttribute('meta.score') ?? $document->getAttribute('score');
            if (\is_numeric($documentScore)) {
                $this->assertContains((int) $documentScore, [10, 313]);
                $this->assertNotSame(8686, (int) $documentScore);
            }
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreNestedAndOrTwoAliasesIndependent(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, , $peerCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $peerCol): void {
            $results = $database->find($mCol, [
                Query::join($peerCol, '$id', 'mainId', '=', 'alpha'),
                Query::join($peerCol, 'peerKey', '$id', '=', 'beta'),
                Query::and([
                    Query::equal('alpha.label', ['alpha-one']),
                    Query::or([
                        Query::equal('beta.label', ['beta-key']),
                        Query::equal('alpha.score', [8686]),
                    ]),
                ]),
                Query::select(['name', 'alpha.$id', 'beta.$id', 'alpha.label', 'beta.label']),
            ]);

            $this->assertSame(1, \count($results));
            $this->assertComboSecretsHidden($results);
            $this->assertSame('hm1', $results[0]->getId());
            $this->assertSame('peer-a', $results[0]->getAttribute('alpha.$id'));
            $this->assertSame('peer-b', $results[0]->getAttribute('beta.$id'));
            $this->assertSame('alpha-one', $results[0]->getAttribute('alpha.label'));
            $this->assertSame('beta-key', $results[0]->getAttribute('beta.label'));
            $this->assertNotSame('peer-a', $results[0]->getId());
            $this->assertNotSame('peer-b', $results[0]->getId());
            $this->assertNotSame('peer-hidden', $results[0]->getId());

            $hiddenOnly = $database->find($mCol, [
                Query::join($peerCol, '$id', 'mainId', '=', 'alpha'),
                Query::join($peerCol, 'peerKey', '$id', '=', 'beta'),
                Query::or([
                    Query::equal('alpha.score', [8686]),
                    Query::equal('beta.secret', ['combo-hard-alpha']),
                ]),
            ]);
            $this->assertSame(0, \count($hiddenOnly));
            $this->assertComboSecretsHidden($hiddenOnly);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreLeftOnVsInnerWhereVsFojNull(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $left = $database->find($mCol, [
                Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
            ]);
            $this->assertGreaterThanOrEqual(3, \count($left));
            $this->assertComboSecretsHidden($left);
            $leftIds = \array_map(static fn (Document $document): string => $document->getId(), $left);
            $this->assertContains('hm1', $leftIds);
            $this->assertContains('hm2', $leftIds);
            $this->assertContains('hm3', $leftIds);
            $leftScores = $this->comboNumericScores($left);
            $this->assertContains(10, $leftScores);
            $this->assertContains(313, $leftScores);
            $this->assertSame(false, \in_array(8686, $leftScores, true));
            $this->assertSame(false, \in_array(42, $leftScores, true));

            $innerHidden = $database->find($mCol, [
                Query::join($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::equal('meta.score', [8686]),
            ]);
            $this->assertSame(0, \count($innerHidden));
            $this->assertComboSecretsHidden($innerHidden);

            $foj = $database->find($mCol, [
                Query::fullOuterJoin($metaCol, '$id', 'mainId', '=', 'meta'),
            ]);
            $this->assertGreaterThanOrEqual(2, \count($foj));
            $this->assertComboSecretsHidden($foj);
            $fojScores = $this->comboNumericScores($foj);
            $this->assertContains(42, $fojScores);
            $this->assertSame(false, \in_array(8686, $fojScores, true));
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreFojPlusSecondAliasCursorRemap(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol, $peerCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol, $peerCol): void {
            $ordered = [
                Query::fullOuterJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::join($peerCol, '$id', 'mainId', '=', 'peer'),
                Query::orderAsc('meta.score'),
                Query::select(['name', 'meta.$id', 'meta.score', 'peer.$id', 'peer.label']),
            ];

            $full = $database->find($mCol, $ordered);
            $this->assertGreaterThanOrEqual(2, \count($full));
            $this->assertComboSecretsHidden($full);

            $fullScores = $this->comboNumericScores($full);
            $this->assertSame(false, \in_array(8686, $fullScores, true));
            $this->assertSame($fullScores, $this->sortedAsc($fullScores));

            foreach ($full as $document) {
                $id = $document->getId();
                $this->assertTrue($id === '' || \in_array($id, ['hm1', 'hm2', 'hm3'], true));
            }

            $cursor = null;
            $cursorIndex = null;
            foreach ($full as $index => $document) {
                $score = $this->comboJoinScore($document);
                if ($document->getId() !== '' && $score !== null) {
                    $cursor = $document;
                    $cursorIndex = $index;
                    break;
                }
            }
            $this->assertNotNull($cursor);
            $this->assertNotSame('', $cursor->getId());
            $this->assertNotNull($cursorIndex);
            $cursorScore = $this->comboJoinScore($cursor);
            $this->assertNotNull($cursorScore);

            $after = $database->find($mCol, [
                ...$ordered,
                Query::cursorAfter($cursor),
                Query::limit(1),
            ]);
            $this->assertSame(1, \count($after));
            $this->assertComboSecretsHidden($after);
            $afterScore = $this->comboJoinScore($after[0]);
            $this->assertNotNull($afterScore);
            $this->assertTrue($after[0]->getId() === '' || \in_array($after[0]->getId(), ['hm1', 'hm2', 'hm3'], true));

            $expectedScore = null;
            for ($index = (int) $cursorIndex + 1, $total = \count($full); $index < $total; $index++) {
                $score = $this->comboJoinScore($full[$index]);
                if ($score === null) {
                    continue;
                }
                if ($score === $cursorScore && $full[$index]->getId() === $cursor->getId()) {
                    continue;
                }
                $expectedScore = $score;
                break;
            }
            $this->assertNotNull($expectedScore);
            $this->assertSame($expectedScore, $afterScore);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreGetDocumentSelectDottedJoinInternals(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $queries = [
                Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::select(['name', 'meta.score', 'meta.$id', 'meta.$permissions']),
            ];

            $hm1 = $database->getDocument($mCol, 'hm1', $queries);
            $this->assertSame(false, $hm1->isEmpty());
            $this->assertSame('hm1', $hm1->getId());
            $this->assertContains($hm1->getAttribute('meta.$id'), ['hm-meta-10', 'hm-meta-313']);
            $this->assertNotSame($hm1->getId(), $hm1->getAttribute('meta.$id'));

            $hm2 = $database->getDocument($mCol, 'hm2', $queries);
            $this->assertSame(false, $hm2->isEmpty());
            $this->assertSame('hm2', $hm2->getId());
            $this->assertSame('hm-meta-20', $hm2->getAttribute('meta.$id'));
            $this->assertNotSame($hm2->getId(), $hm2->getAttribute('meta.$id'));

            foreach ([$hm1, $hm2] as $document) {
                $score = $document->getAttribute('meta.score') ?? $document->getAttribute('score');
                if (\is_numeric($score)) {
                    $this->assertNotSame(8686, (int) $score);
                }
                $permissions = $document->getAttribute('meta.$permissions');
                if (\is_string($permissions)) {
                    $permissions = \json_decode($permissions, true);
                }
                if (\is_array($permissions)) {
                    foreach ($permissions as $permission) {
                        if (\is_string($permission)) {
                            $this->assertSame(false, \str_contains($permission, 'user:combo-hard-hidden'));
                        }
                    }
                }
            }

            $this->assertComboSecretsHidden([$hm1, $hm2]);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreCountSumFojExcludesSecret(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $foj = [
                Query::fullOuterJoin($metaCol, '$id', 'mainId', '=', 'meta'),
            ];
            $found = $database->find($mCol, $foj);
            $this->assertGreaterThanOrEqual(2, \count($found));
            $this->assertComboSecretsHidden($found);
            $this->assertSame(\count($found), $database->count($mCol, $foj));
            $this->assertSame(false, \in_array(8686, $this->comboNumericScores($found), true));
            $this->assertContains(42, $this->comboNumericScores($found));

            $this->assertSame(0, $database->count($mCol, [
                Query::fullOuterJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::equal('meta.score', [8686]),
            ]));

            $sum = $database->sum($mCol, 'meta.score', $foj);
            $this->assertSame(400, (int) $sum);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreIsNotNullNotEqualSecretDoesNotLeak(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol): void {
            $notNull = $database->find($mCol, [
                Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::isNotNull('meta.secret'),
                Query::select(['name', 'meta.score', 'meta.secret']),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($notNull));
            $this->assertComboSecretsHidden($notNull);

            $secrets = [];
            foreach ($notNull as $document) {
                $secret = $document->getAttribute('meta.secret') ?? $document->getAttribute('secret');
                if (\is_string($secret) && $secret !== '') {
                    $secrets[] = $secret;
                }
                $score = $document->getAttribute('meta.score') ?? $document->getAttribute('score');
                if (\is_numeric($score)) {
                    $this->assertNotSame(8686, (int) $score);
                }
            }
            $this->assertContains('visible', $secrets);
            $this->assertContains('visible-313', $secrets);
            $this->assertContains('visible-20', $secrets);
            $this->assertContains('visible-15', $secrets);
            $this->assertSame(false, \in_array('combo-hard-alpha', $secrets, true));

            $notEqual = $database->find($mCol, [
                Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::notEqual('meta.score', 8686),
                Query::select(['name', 'meta.score', 'meta.secret']),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($notEqual));
            $this->assertComboSecretsHidden($notEqual);
            $this->assertSame(false, \in_array(8686, $this->comboNumericScores($notEqual), true));

            $notContains = $database->find($mCol, [
                Query::leftJoin($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::notContains('meta.secret', ['combo-hard-alpha']),
                Query::select(['name', 'meta.score', 'meta.secret']),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($notContains));
            $this->assertComboSecretsHidden($notContains);
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
    }

    public function testJoinHardcoreStaleJoinAliasRejectedOnSecondFind(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$mCol, $metaCol, $peerCol] = $this->seedJoinHardcoreFixture($database);

        $this->withComboRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $metaCol, $peerCol): void {
            $first = $database->find($mCol, [
                Query::join($metaCol, '$id', 'mainId', '=', 'meta'),
                Query::equal('meta.score', [10]),
            ]);
            $this->assertSame(1, \count($first));
            $this->assertComboSecretsHidden($first);
            $this->assertSame('hm1', $first[0]->getId());
            $this->assertSame(false, \in_array(8686, $this->comboNumericScores($first), true));

            try {
                $database->find($mCol, [
                    Query::join($peerCol, '$id', 'mainId', '=', 'peer'),
                    Query::equal('meta.score', [8686]),
                ]);
                $this->fail('Expected QueryException for stale join alias');
            } catch (QueryException $exception) {
                $this->assertStringContainsString('Attribute not found', $exception->getMessage());
            }
        });

        $this->cleanupAggCollections($database, $this->joinHardcoreCollections());
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

        $database->createCollection(new Collection(id: $mCol, permissions: $any, documentSecurity: false));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $pubCol, permissions: $any));
        $database->createAttribute($pubCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($pubCol, Attribute::integer(key: 'score', required: true));

        $database->createCollection(new Collection(id: $secCol, permissions: $any));
        $database->createAttribute($secCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($secCol, Attribute::integer(key: 'score', required: true));
        $database->createAttribute($secCol, Attribute::string(key: 'secret', size: 100));

        $database->createCollection(new Collection(id: $selfCol, permissions: $any));
        $database->createAttribute($selfCol, Attribute::string(key: 'payload', size: 100, required: true));
        $database->createAttribute($selfCol, Attribute::string(key: 'tag', size: 50, required: true));
        $database->createAttribute($selfCol, Attribute::string(key: 'mainId'));

        $database->createCollection(new Collection(id: $cCol, permissions: $any));
        $database->createAttribute($cCol, Attribute::string(key: 'selfId', required: true));
        $database->createAttribute($cCol, Attribute::string(key: 'secret', size: 100, required: true));

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
        $this->assertNotSame('hm-meta-secret', $document->getId());
        $this->assertNotSame('peer-hidden', $document->getId());
        $this->assertNotSame('hc-hidden', $document->getId());

        foreach (['score', 'meta.score', 'sec.score', 'pub.score', 'rev.score', 'c.score', 'tail.score', 'alpha.score', 'beta.score', 'peer.score'] as $scoreKey) {
            $score = $document->getAttribute($scoreKey);
            if (\is_numeric($score)) {
                $this->assertNotSame(777, (int) $score);
                $this->assertNotSame(8686, (int) $score);
                $this->assertNotSame(5151, (int) $score);
            }
        }

        foreach (['secret', 'payload', 'meta.secret', 'sec.secret', 'c.secret', 'tail.secret', 'alpha.secret', 'beta.secret', 'peer.secret'] as $secretKey) {
            $this->assertNotSame('combo-secret-alpha', $document->getAttribute($secretKey));
            $this->assertNotSame('combo-hard-alpha', $document->getAttribute($secretKey));
        }

        foreach ($document->getPermissions() as $permission) {
            $this->assertSame(false, \str_contains($permission, 'j-combo-secret'));
            $this->assertSame(false, \str_contains($permission, 'c-combo-secret'));
            $this->assertSame(false, \str_contains($permission, 'user:combo-hidden'));
            $this->assertSame(false, \str_contains($permission, 'combo-secret-perm'));
            $this->assertSame(false, \str_contains($permission, 'user:combo-hard-hidden'));
            $this->assertSame(false, \str_contains($permission, 'combo-hard-alpha'));
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
        $this->assertSame(false, \str_contains($encoded, 'combo-hard-alpha'));
        $this->assertSame(false, \str_contains($encoded, 'user:combo-hard-hidden'));
        $this->assertSame(false, $this->comboEncodedJsonContainsScalar($encoded, 777));
        $this->assertSame(false, $this->comboEncodedJsonContainsScalar($encoded, 8686));
        $this->assertSame(false, $this->comboEncodedJsonContainsScalar($encoded, 5151));
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
        $name = \is_string($key) && \str_contains($key, '.')
            ? \substr($key, (int) \strrpos($key, '.') + 1)
            : $key;

        return \in_array($name, [
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
            $score = $this->comboJoinScore($document);
            if ($score !== null) {
                $scores[] = $score;
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

    /**
     * @param list<int> $scores
     * @return list<int>
     */
    private function sortedAsc(array $scores): array
    {
        $sorted = $scores;
        \sort($sorted, SORT_NUMERIC);

        return $sorted;
    }

    private function comboJoinScore(Document $document): ?int
    {
        $score = $document->getAttribute('pub.score')
            ?? $document->getAttribute('sec.score')
            ?? $document->getAttribute('rev.score')
            ?? $document->getAttribute('meta.score')
            ?? $document->getAttribute('peer.score')
            ?? $document->getAttribute('tail.score')
            ?? $document->getAttribute('alpha.score')
            ?? $document->getAttribute('beta.score')
            ?? $document->getAttribute('score');
        if (! \is_numeric($score)) {
            return null;
        }

        return (int) $score;
    }

    /**
     * @return list<string>
     */
    private function joinHardcoreCollections(): array
    {
        return ['jh_m', 'jh_meta', 'jh_peer', 'jh_a', 'jh_b', 'jh_c'];
    }

    /**
     * @return array{0: string, 1: string, 2: string, 3: string, 4: string, 5: string}
     */
    private function seedJoinHardcoreFixture(Database $database): array
    {
        $mCol = 'jh_m';
        $metaCol = 'jh_meta';
        $peerCol = 'jh_peer';
        $aCol = 'jh_a';
        $bCol = 'jh_b';
        $cCol = 'jh_c';
        $this->cleanupAggCollections($database, [$mCol, $metaCol, $peerCol, $aCol, $bCol, $cCol]);

        $any = [Permission::create(Role::any()), Permission::read(Role::any())];
        $readAny = [Permission::read(Role::any())];
        $hidden = [Permission::read(Role::user('combo-hard-hidden'))];

        $database->createCollection(new Collection(id: $mCol, permissions: $any, documentSecurity: false));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createAttribute($mCol, Attribute::integer(key: 'rank', required: true));
        $database->createAttribute($mCol, Attribute::string(key: 'peerKey'));

        $database->createCollection(new Collection(id: $metaCol, permissions: $any));
        $database->createAttribute($metaCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($metaCol, Attribute::integer(key: 'score', required: true));
        $database->createAttribute($metaCol, Attribute::string(key: 'secret', size: 100));
        $database->createAttribute($metaCol, Attribute::string(key: 'label', size: 100));
        $database->createAttribute($metaCol, Attribute::string(key: 'body'));

        $database->createCollection(new Collection(id: $peerCol, permissions: $any));
        $database->createAttribute($peerCol, Attribute::string(key: 'mainId'));
        $database->createAttribute($peerCol, Attribute::string(key: 'label', size: 100, required: true));
        $database->createAttribute($peerCol, Attribute::integer(key: 'score', required: true));
        $database->createAttribute($peerCol, Attribute::string(key: 'secret', size: 100));

        $database->createCollection(new Collection(id: $aCol, permissions: $any));
        $database->createAttribute($aCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $bCol, permissions: $any, documentSecurity: false));
        $database->createAttribute($bCol, Attribute::string(key: 'aId', required: true));
        $database->createAttribute($bCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($bCol, Attribute::string(key: 'label', size: 100, required: true));

        $database->createCollection(new Collection(id: $cCol, permissions: $any));
        $database->createAttribute($cCol, Attribute::string(key: 'bId', required: true));
        $database->createAttribute($cCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($cCol, Attribute::string(key: 'secret', size: 100, required: true));
        $database->createAttribute($cCol, Attribute::integer(key: 'score', required: true));

        if ($database->getAdapter()->supports(Capability::Fulltext)) {
            $database->createIndex($metaCol, Index::fullText(key: 'idx_jh_meta_body', attributes: ['body']));
        }

        $database->createDocument($mCol, new Document([
            '$id' => 'hm1',
            'name' => 'Main',
            'rank' => 1,
            'peerKey' => 'peer-b',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'hm2',
            'name' => 'Second',
            'rank' => 2,
            'peerKey' => 'peer-missing',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'hm3',
            'name' => 'Third',
            'rank' => 1,
            'peerKey' => 'peer-a',
            '$permissions' => $readAny,
        ]));

        $database->createDocument($metaCol, new Document([
            '$id' => 'hm-meta-10',
            'mainId' => 'hm1',
            'score' => 10,
            'secret' => 'visible',
            'label' => 'visible-ten',
            'body' => 'hard-needle visible',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($metaCol, new Document([
            '$id' => 'hm-meta-313',
            'mainId' => 'hm1',
            'score' => 313,
            'secret' => 'visible-313',
            'label' => 'visible-high',
            'body' => 'other text',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($metaCol, new Document([
            '$id' => 'hm-meta-20',
            'mainId' => 'hm2',
            'score' => 20,
            'secret' => 'visible-20',
            'label' => 'visible-m2',
            'body' => 'm2 text',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($metaCol, new Document([
            '$id' => 'hm-meta-15',
            'mainId' => 'hm3',
            'score' => 15,
            'secret' => 'visible-15',
            'label' => 'visible-third',
            'body' => 'third text',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($metaCol, new Document([
            '$id' => 'hm-meta-secret',
            'mainId' => 'hm1',
            'score' => 8686,
            'secret' => 'combo-hard-alpha',
            'label' => 'hidden-label',
            'body' => 'hidden-search',
            '$permissions' => $hidden,
        ]));
        $database->createDocument($metaCol, new Document([
            '$id' => 'hm-meta-orphan',
            'mainId' => 'missing',
            'score' => 42,
            'secret' => 'orphan-visible',
            'label' => 'orphan',
            'body' => 'orphan text',
            '$permissions' => $readAny,
        ]));

        $database->createDocument($peerCol, new Document([
            '$id' => 'peer-a',
            'mainId' => 'hm1',
            'label' => 'alpha-one',
            'score' => 11,
            '$permissions' => $readAny,
        ]));
        $database->createDocument($peerCol, new Document([
            '$id' => 'peer-b',
            'mainId' => 'hm2',
            'label' => 'beta-key',
            'score' => 22,
            '$permissions' => $readAny,
        ]));
        $database->createDocument($peerCol, new Document([
            '$id' => 'peer-c',
            'mainId' => 'hm1',
            'label' => 'alpha-two',
            'score' => 33,
            '$permissions' => $readAny,
        ]));
        $database->createDocument($peerCol, new Document([
            '$id' => 'peer-hidden',
            'mainId' => 'hm1',
            'label' => 'combo-hard-alpha',
            'score' => 8686,
            'secret' => 'combo-hard-alpha',
            '$permissions' => $hidden,
        ]));

        $database->createDocument($aCol, new Document([
            '$id' => 'ha1',
            'name' => 'Alpha',
            '$permissions' => $readAny,
        ]));

        $database->createDocument($bCol, new Document([
            '$id' => 'hb1',
            'aId' => 'ha1',
            'mainId' => 'hm1',
            'label' => 'b-public',
            '$permissions' => $readAny,
        ]));
        $database->createDocument($bCol, new Document([
            '$id' => 'hb2',
            'aId' => 'ha1',
            'mainId' => 'hm2',
            'label' => 'b-second',
            '$permissions' => $readAny,
        ]));

        $database->createDocument($cCol, new Document([
            '$id' => 'hc-open',
            'bId' => 'hb1',
            'mainId' => 'hm1',
            'secret' => 'c-open-token',
            'score' => 1,
            '$permissions' => $readAny,
        ]));
        $database->createDocument($cCol, new Document([
            '$id' => 'hc-hidden',
            'bId' => 'hb1',
            'mainId' => 'hm1',
            'secret' => 'combo-hard-alpha',
            'score' => 8686,
            '$permissions' => $hidden,
        ]));
        $database->createDocument($cCol, new Document([
            '$id' => 'hc-right',
            'bId' => 'missing',
            'mainId' => 'missing',
            'secret' => 'c-right-open',
            'score' => 7,
            '$permissions' => $readAny,
        ]));

        return [$mCol, $metaCol, $peerCol, $aCol, $bCol, $cCol];
    }

    private function joinHardcoreHasFulltextIndex(Database $database, string $collection): bool
    {
        if (! $database->getAdapter()->supports(Capability::Fulltext)) {
            return false;
        }

        /** @var array<Document> $indexes */
        $indexes = $database->getCollection($collection)->indexes;
        foreach ($indexes as $index) {
            $type = $index->getAttribute('type');
            $typeValue = $type instanceof IndexType ? $type->value : $type;
            if ($typeValue === IndexType::Fulltext->value) {
                return true;
            }
        }

        return false;
    }
}
