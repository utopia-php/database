<?php

namespace Tests\Unit\Hook;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Hook\Tenancy;
use Utopia\Database\Storage;

final class TenancyHookTest extends TestCase
{
    /**
     * @return array<string, array{int|string|null}>
     */
    public static function tenants(): array
    {
        return [
            'integer' => [5],
            'string' => ['tenant-a'],
            'per document' => [null],
        ];
    }

    #[DataProvider('tenants')]
    public function testGetTenantReturnsTheAmbientTenant(int|string|null $tenant): void
    {
        $this->assertSame($tenant, (new Tenancy($tenant))->getTenant());
    }

    public function testDecorateRowPrefersTheMetadataTenantOverTheAmbientOne(): void
    {
        $hook = new Tenancy(5);

        $this->assertSame(['title' => 'x', Storage::TENANT => 7], $hook->decorateRow(['title' => 'x'], ['tenant' => 7]));
        $this->assertSame(['title' => 'x', Storage::TENANT => 5], $hook->decorateRow(['title' => 'x']));
        $this->assertSame(['title' => 'x', 'owner' => 5], (new Tenancy(5, 'owner'))->decorateRow(['title' => 'x']));
    }
}
