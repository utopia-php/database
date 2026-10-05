<?php

namespace Tests\Unit;

trait MagicAccessAssertions
{
    private function assertNoMagicAccess(MagicAccessRecorder $recorder, string $operation): void
    {
        $this->assertSame([], $recorder->reads, $operation.' read '.self::accessed($recorder->reads).' through __get, which the PHP 8.5 tracing JIT crashes on (php/php-src#22084)');
        $this->assertSame([], $recorder->writes, $operation.' wrote '.self::accessed($recorder->writes).' through __set, which the PHP 8.5 tracing JIT crashes on (php/php-src#22084)');
        $this->assertSame([], $recorder->hookReads, $operation.' read '.self::accessed($recorder->hookReads).' through a property hook, which the PHP 8.5 tracing JIT crashes on (php/php-src#22084)');
    }

    /**
     * @param  list<string>  $accesses
     */
    private static function accessed(array $accesses): string
    {
        return \implode(', ', \array_values(\array_unique($accesses)));
    }
}
