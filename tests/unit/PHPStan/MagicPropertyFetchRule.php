<?php

namespace Tests\Unit\PHPStan;

use PhpParser\Node;
use PhpParser\Node\Expr\PropertyFetch;
use PhpParser\Node\Expr\Variable;
use PhpParser\Node\Identifier;
use PHPStan\Analyser\Scope;
use PHPStan\File\FileHelper;
use PHPStan\Reflection\ClassReflection;
use PHPStan\Rules\IdentifierRuleError;
use PHPStan\Rules\Rule;
use PHPStan\Rules\RuleErrorBuilder;
use PHPStan\Type\Constant\ConstantStringType;
use Utopia\Database\PDOStatement;

/**
 * Guards php/php-src#22084: the PHP 8.5 tracing JIT miscompiles property reads that route through __get/__set/__isset
 * or through a property hook, so src must not reach either.
 *
 * @implements Rule<PropertyFetch>
 */
final readonly class MagicPropertyFetchRule implements Rule
{
    public const string MAGIC_IDENTIFIER = 'utopiaDatabase.magicProperty';

    public const string HOOKED_IDENTIFIER = 'utopiaDatabase.hookedProperty';

    /**
     * Classes in src that still resolve inaccessible properties through __get/__set/__isset.
     *
     * @var list<class-string>
     */
    private const array MAGIC_CLASSES = [
        PDOStatement::class,
    ];

    private string $sourceDirectory;

    /**
     * @throws \InvalidArgumentException When the source directory does not exist
     */
    public function __construct(string $sourceDirectory, FileHelper $fileHelper)
    {
        $directory = $fileHelper->normalizePath($fileHelper->absolutizePath($sourceDirectory));
        if (! \is_dir($directory)) {
            throw new \InvalidArgumentException(\sprintf('Source directory "%s" does not exist, so the rule would report nothing.', $sourceDirectory));
        }

        $this->sourceDirectory = \rtrim($directory, '/').'/';
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
        if (! \str_starts_with($scope->getFile(), $this->sourceDirectory)) {
            return [];
        }

        $classes = $scope->getType($node->var)->getObjectClassReflections();

        $errors = [];
        foreach ($this->namesOf($node, $scope) as $name) {
            foreach ($classes as $class) {
                if ($this->isMagic($class, $name, $scope)) {
                    $errors[] = RuleErrorBuilder::message(\sprintf(
                        'Property %s::$%s is reached through __get/__set/__isset, which the PHP 8.5 tracing JIT miscompiles (php/php-src#22084). Use the wrapped \PDOStatement directly instead.',
                        $class->getDisplayName(),
                        $name,
                    ))->identifier(self::MAGIC_IDENTIFIER)->build();
                } elseif ($this->isHooked($class, $name) && ! $this->isInsideOwnHook($node, $scope, $name)) {
                    $errors[] = RuleErrorBuilder::message(\sprintf(
                        'Property %s::$%s is reached through its hook, which the PHP 8.5 tracing JIT miscompiles (php/php-src#22084). Use a plain property or a method instead.',
                        $class->getNativeProperty($name)->getDeclaringClass()->getDisplayName(),
                        $name,
                    ))->identifier(self::HOOKED_IDENTIFIER)->build();
                }
            }
        }

        return $errors;
    }

    /**
     * @return list<string>
     */
    private function namesOf(PropertyFetch $node, Scope $scope): array
    {
        if ($node->name instanceof Identifier) {
            return [$node->name->toString()];
        }

        return \array_map(
            static fn (ConstantStringType $name): string => $name->getValue(),
            $scope->getType($node->name)->getConstantStrings(),
        );
    }

    private function isMagic(ClassReflection $class, string $name, Scope $scope): bool
    {
        foreach (self::MAGIC_CLASSES as $magic) {
            if ($class->is($magic)) {
                return ! $class->hasNativeProperty($name) || ! $scope->canAccessProperty($class->getNativeProperty($name));
            }
        }

        return false;
    }

    private function isHooked(ClassReflection $class, string $name): bool
    {
        return $class->hasNativeProperty($name) && $class->getNativeProperty($name)->isHooked();
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
}
