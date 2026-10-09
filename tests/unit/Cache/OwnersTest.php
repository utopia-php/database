<?php

namespace Tests\Unit\Cache;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Cache\Owners;

class OwnersTest extends TestCase
{
    private const string KEY = 'default:collection';

    private const int TTL = 3600;

    public function testRegisteringAndFindingOwnersListsTheHashOnce(): void
    {
        $cache = new class (new RedisLeasableCache()) extends Cache {
            public int $lists = 0;

            /** @return string[] */
            #[\Override]
            public function list(string $key): array
            {
                $this->lists++;

                return parent::list($key);
            }
        };
        $tokens = ['first', 'second', 'third'];

        foreach ($tokens as $token) {
            $this->assertTrue((new Owners($cache))->register(self::KEY, $token));
        }

        foreach ($tokens as $token) {
            $registration = (new Owners($cache))->find(self::KEY, $token);

            $this->assertSame([self::KEY.'#owners', $token], [$registration->key, $registration->field]);
            $this->assertSame($token, $cache->load($registration->key, self::TTL, $registration->field));
        }

        $this->assertSame(1, $cache->lists, 'Only the first registration should list the owners hash');
    }

    public function testOwnersResolveOnACacheWithoutFields(): void
    {
        $cache = new class (new Memory()) extends Cache {
            public int $lists = 0;

            /** @return string[] */
            #[\Override]
            public function list(string $key): array
            {
                $this->lists++;

                return parent::list($key);
            }
        };

        foreach (['first', 'second'] as $token) {
            $this->assertTrue((new Owners($cache))->register(self::KEY, $token));

            $registration = (new Owners($cache))->find(self::KEY, $token);

            $this->assertSame([self::KEY.'#owner:'.$token, ''], [$registration->key, $registration->field]);
            $this->assertSame($token, $cache->load($registration->key, self::TTL, $registration->field));
        }

        $this->assertSame(4, $cache->lists, 'A cache that keeps no fields should be asked again on every registration');
    }

    public function testAFlushBeforeTheFirstListDoesNotPinPerTokenKeys(): void
    {
        $cache = new class (new RedisLeasableCache()) extends Cache {
            private bool $flushed = false;

            /** @return string[] */
            #[\Override]
            public function list(string $key): array
            {
                if (! $this->flushed) {
                    $this->flushed = true;
                    $this->flush();
                }

                return parent::list($key);
            }
        };

        $this->assertTrue((new Owners($cache))->register(self::KEY, 'flushed'));
        $flushed = (new Owners($cache))->find(self::KEY, 'flushed');
        $this->assertSame([self::KEY.'#owner:flushed', ''], [$flushed->key, $flushed->field]);

        $this->assertTrue((new Owners($cache))->register(self::KEY, 'kept'));
        $kept = (new Owners($cache))->find(self::KEY, 'kept');
        $this->assertSame(
            [self::KEY.'#owners', 'kept'],
            [$kept->key, $kept->field],
            'A cache that listed no field once should still register later owners as fields',
        );
    }
}
