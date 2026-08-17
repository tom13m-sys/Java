package Processors;

import Data.Base.EventBase;
import Processors.Base.ComponentFactory;
import Processors.Base.ProcessorBase;
import Validators.Base.ValidatorBase;
import java.util.HashMap;
import java.util.Map;

public class PipelineRegistry {
    // A registry holding factories using wildcard capturing to ensure loose coupling
    private final Map<Class<? extends EventBase>, ComponentFactory<?, ?, ?>> registry = new HashMap<>();

    // Strongly-typed registration method ensures compile-time validation of types
    public <E extends EventBase, V extends ValidatorBase, P extends ProcessorBase> void register(
            Class<E> eventClass, ComponentFactory<E, V, P> factory) {
        registry.put(eventClass, factory);
    }

    @SuppressWarnings("unchecked")
    public <E extends EventBase> ComponentFactory<E, ValidatorBase, ProcessorBase> getFactory(Class<E> eventClass) {
        ComponentFactory<?, ?, ?> factory = registry.get(eventClass);
        if (factory == null) {
            throw new IllegalArgumentException("No pipeline factory registered for: " + eventClass.getSimpleName());
        }
        // Safe cast handled by our strongly-typed register method constraint
        return (ComponentFactory<E, ValidatorBase, ProcessorBase>) factory;
    }
}