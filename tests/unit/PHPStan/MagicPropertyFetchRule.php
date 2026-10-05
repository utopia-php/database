<?php

namespace Tests\Unit\PHPStan;

use PhpParser\Node;
use PhpParser\Node\Expr\PropertyFetch;
use PhpParser\Node\Expr\Variable;
use PhpParser\Node\Identifier;
use PHPStan\Analyser\Scope;
use PHPStan\Reflection\ClassReflection;
use PHPStan\Rules\IdentifierRuleError;
use PHPStan\Rules\Rule;
use PHPStan\Rules\RuleErrorBuilder;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Index;
use Utopia\Database\PDOStatement;
use Utopia\Database\Relationship;

/**
 * @implements Rule<PropertyFetch>
 */
final readonly class MagicPropertyFetchRule implements Rule
{
    public const string MAGIC_IDENTIFIER = 'utopiaDatabase.magicProperty';

    public const string HOOKED_IDENTIFIER = 'utopiaDatabase.hookedProperty';

    private const string ISSUE = 'php/php-src#22084';

    /**
     * @var array<class-string, array<string, string>>
     */
    private const array GETTERS = [
        Attribute::class => [
            'key' => 'getKey()',
            'type' => 'getType()',
            'size' => 'getSize()',
            'required' => 'isRequired()',
            'default' => 'getDefault()',
            'signed' => 'isSigned()',
            'array' => 'isArray()',
            'format' => 'getFormat()',
            'formatOptions' => 'getFormatOptions()',
            'filters' => 'getFilters()',
            'status' => 'getStatus()',
            'options' => 'getOptions()',
        ],
        Collection::class => [
            'id' => 'getId()',
            'attributes' => 'getDeclaredAttributes()',
            'indexes' => 'getIndexes()',
            'permissions' => 'getPermissions()',
        ],
        Index::class => [
            'key' => 'getKey()',
            'type' => 'getType()',
            'attributes' => 'getIndexedAttributes()',
            'lengths' => 'getLengths()',
            'orders' => 'getOrders()',
            'ttl' => 'getTtl()',
        ],
        PDOStatement::class => [],
        Relationship::class => [
            'collection' => 'getSourceCollection()',
            'relatedCollection' => 'getRelatedCollection()',
            'type' => 'getType()',
            'twoWay' => 'isTwoWay()',
            'key' => 'getKey()',
            'twoWayKey' => 'getTwoWayKey()',
            'onDelete' => 'getOnDelete()',
            'side' => 'getSide()',
        ],
    ];

    private string $sourceDirectory;

    public function __construct(string $sourceDirectory)
    {
        $this->sourceDirectory = \rtrim($sourceDirectory, '/') . '/';
    }

    public function getNodeType(): string
    {
        return PropertyFetch::class;
    }

    /**
     * @return list<IdentifierRuleError>
     */
    public function processNode(Node $node, Scope $scope): array
    {
        if (! $node->name instanceof Identifier) {
            return [];
        }
        if (! \str_starts_with($scope->getFile(), $this->sourceDirectory)) {
            return [];
        }

        $name = $node->name->toString();
        $classes = $scope->getType($node->var)->getObjectClassReflections();

        $errors = [];
        $magic = $this->findMagicClass($classes, $name);
        if ($magic !== null) {
            [$class, $model] = $magic;
            $errors[] = $this->magicError($class, $model, $name, $scope->isInExpressionAssign($node));
        }
        $hooked = $this->findHookedClass($classes, $name);
        if ($hooked !== null && ! $this->isInsideOwnHook($node, $scope, $name)) {
            $errors[] = $this->hookedError($hooked, $name);
        }

        return $errors;
    }

    /**
     * @param  list<ClassReflection>  $classes
     * @return array{ClassReflection, class-string}|null
     */
    private function findMagicClass(array $classes, string $name): ?array
    {
        foreach ($classes as $class) {
            $model = $this->modelOf($class);
            if ($model !== null && ! $class->hasNativeProperty($name)) {
                return [$class, $model];
            }
        }

        return null;
    }

    /**
     * @param  list<ClassReflection>  $classes
     */
    private function findHookedClass(array $classes, string $name): ?ClassReflection
    {
        foreach ($classes as $class) {
            if (! $class->hasNativeProperty($name)) {
                continue;
            }
            $property = $class->getNativeProperty($name);
            if ($property->isHooked()) {
                return $property->getDeclaringClass();
            }
        }

        return null;
    }

    /**
     * @return class-string|null
     */
    private function modelOf(ClassReflection $class): ?string
    {
        foreach (\array_keys(self::GETTERS) as $model) {
            if ($class->is($model)) {
                return $model;
            }
        }

        return null;
    }

    private function isInsideOwnHook(PropertyFetch $node, Scope $scope, string $name): bool
    {
        $function = $scope->getFunction();

        return $function !== null
            && $function->isMethodOrPropertyHook()
            && $function->isPropertyHook()
            && $function->getHookedPropertyName() === $name
            && $node->var instanceof Variable
            && $node->var->name === 'this';
    }

    /**
     * @param  class-string  $model
     */
    private function magicError(ClassReflection $class, string $model, string $name, bool $isWrite): IdentifierRuleError
    {
        return RuleErrorBuilder::message(\sprintf(
            'Magic property %s %s::$%s routes through __get/__set/__isset, which the PHP 8.5 tracing JIT miscompiles (%s). Use %s instead.',
            $isWrite ? 'write to' : 'fetch of',
            $class->getDisplayName(),
            $name,
            self::ISSUE,
            $this->replacementFor($model, $name, $isWrite),
        ))
            ->identifier(self::MAGIC_IDENTIFIER)
            ->build();
    }

    /**
     * @param  class-string  $model
     */
    private function replacementFor(string $model, string $name, bool $isWrite): string
    {
        if ($model === PDOStatement::class) {
            return 'the wrapped \\PDOStatement directly';
        }
        if ($isWrite) {
            return 'setAttribute()';
        }

        return self::GETTERS[$model][$name] ?? "getAttribute('{$name}')";
    }

    private function hookedError(ClassReflection $class, string $name): IdentifierRuleError
    {
        return RuleErrorBuilder::message(\sprintf(
            'Hooked property %s::$%s is accessed through its hook, which the PHP 8.5 tracing JIT miscompiles (%s). Use a plain property or a method instead.',
            $class->getDisplayName(),
            $name,
            self::ISSUE,
        ))
            ->identifier(self::HOOKED_IDENTIFIER)
            ->build();
    }
}
