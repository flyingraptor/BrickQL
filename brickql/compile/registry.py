"""Compiler and operator registries (Open/Closed Principle).

Before this module existed, adding a new dialect required editing
``__init__.py``, and adding a new operator required editing both
``builder.py`` and ``validator.py``.  These registries allow extension
without modification.

``CompilerFactory``
    Central registry for :class:`~brickql.compile.base.SQLCompiler`
    implementations.  Register a new compiler once; the pipeline looks
    it up automatically.

``OperatorRegistry``
    Per-operator SQL rendering handlers.  The predicate builder queries
    this registry so new operators can be added without touching
    :class:`~brickql.compile.expression_builder.PredicateBuilder`.

Usage::

    from brickql.compile.registry import CompilerFactory

    @CompilerFactory.register("mysql")
    class MySQLCompiler(SQLCompiler):
        ...
"""

from __future__ import annotations

from collections.abc import Callable
from typing import ClassVar

from brickql.compile.base import SQLCompiler
from brickql.errors import CompilationError

# ---------------------------------------------------------------------------
# Compiler factory
# ---------------------------------------------------------------------------


class CompilerFactory:
    """Registry mapping dialect target names to :class:`SQLCompiler` classes.

    Callers register a compiler class once; the pipeline creates instances
    on demand via :meth:`create`.

    Example::

        @CompilerFactory.register("mysql")
        class MySQLCompiler(SQLCompiler):
            ...

        compiler = CompilerFactory.create("mysql")
    """

    _compilers: ClassVar[dict[str, type[SQLCompiler]]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[type[SQLCompiler]], type[SQLCompiler]]:
        """Decorator that registers a compiler class under ``name``.

        Args:
            name: The dialect target name (e.g. ``"postgres"``).

        Returns:
            A decorator that registers and returns the compiler class.
        """

        def decorator(compiler_cls: type[SQLCompiler]) -> type[SQLCompiler]:
            cls._compilers[name] = compiler_cls
            return compiler_cls

        return decorator

    @classmethod
    def register_class(cls, name: str, compiler_cls: type[SQLCompiler]) -> None:
        """Register a compiler class without using the decorator form.

        Args:
            name: The dialect target name.
            compiler_cls: The :class:`SQLCompiler` subclass to register.
        """
        cls._compilers[name] = compiler_cls

    @classmethod
    def create(cls, name: str) -> SQLCompiler:
        """Instantiate the compiler registered for ``name``.

        Args:
            name: The dialect target name.

        Returns:
            A fresh :class:`SQLCompiler` instance.

        Raises:
            CompilationError: If no compiler is registered for ``name``.
        """
        compiler_cls = cls._compilers.get(name)
        if compiler_cls is None:
            registered = sorted(cls._compilers)
            raise CompilationError(
                f"Unsupported dialect target: '{name}'. Registered targets: {registered}."
            )
        return compiler_cls()

    @classmethod
    def registered_targets(cls) -> list[str]:
        """Return the sorted list of registered dialect target names."""
        return sorted(cls._compilers)


# ---------------------------------------------------------------------------
# Operator registry
# ---------------------------------------------------------------------------

#: Type alias for a predicate rendering handler.
#: ``(op_name, args, build_operand_fn) -> sql_string``
OperatorHandler = Callable[[str, object, Callable], str]


class OperatorRegistry:
    """Registry mapping operator names to SQL rendering handlers.

    New operators can be registered without modifying
    :class:`~brickql.compile.expression_builder.PredicateBuilder`.

    Example::

        @OperatorRegistry.register("REGEXP")
        def _regexp_handler(op, args, build_operand):
            left = build_operand(args[0])
            right = build_operand(args[1])
            return f"{left} REGEXP {right}"
    """

    _operators: ClassVar[dict[str, OperatorHandler]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[OperatorHandler], OperatorHandler]:
        """Decorator that registers an operator handler under ``name``.

        Args:
            name: The operator key (e.g. ``"REGEXP"``).

        Returns:
            A decorator that registers and returns the handler.
        """

        def decorator(handler: OperatorHandler) -> OperatorHandler:
            cls._operators[name] = handler
            return handler

        return decorator

    @classmethod
    def register_handler(cls, name: str, handler: OperatorHandler) -> None:
        """Register an operator handler without using the decorator form."""
        cls._operators[name] = handler

    @classmethod
    def get(cls, name: str) -> OperatorHandler | None:
        """Return the handler for ``name``, or ``None`` if not registered."""
        return cls._operators.get(name)

    @classmethod
    def registered_operators(cls) -> list[str]:
        """Return the sorted list of registered operator names."""
        return sorted(cls._operators)
