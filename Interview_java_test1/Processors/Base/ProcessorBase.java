package Processors.Base;

import Data.Base.EventBase;
import Data.Base.ResultBase;

public abstract class ProcessorBase<T> {
    
    public abstract ResultBase processEvent(EventBase event);

}
