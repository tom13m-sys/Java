

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.xml.validation.Validator;

import Data.Base.EventBase;
import Data.Base.ResultBase;
import Processors.LoginProcessor;
import Processors.PipelineRegistry;
import Processors.PurchaseProcessor;
import Processors.RefundProcessor;
import Processors.Base.ComponentFactory;
import Processors.Base.ProcessorBase;
import Validators.LoginValidator;
import Validators.PurchaseValidator;
import Validators.RefundValidator;
import Validators.Base.ValidatorBase;
import Data.FinancialTracker;
import Data.LoginEvent;
import Data.PurchaseEvent;
import Data.RefundEvent;

public class events_stream_interview_q {
    

    private static EventBase getNextEvent() {
        return eventQueue.poll();
    }

    public static void runETLLoop() {
        while (!eventQueue.isEmpty()) {
            EventBase event = getNextEvent();
            if (event == null) break;

            // Fetch the generic handlers dynamically in O(1) time
            ComponentFactory<EventBase, ValidatorBase, ProcessorBase> factory = pipeline.getFactory(event.getClass());
            
            ValidatorBase validator = factory.createValidator(event);

            if (validator.isEventValid()) {
                ProcessorBase processor = factory.createProcessor(tracker);
                ResultBase result = processor.processEvent(event);

                System.out.printf("%s -> Current Metrics [Running Total: %.2f, Logins: %d]%n", 
                    event.toString(), tracker.getRunningTotal(), tracker.getLoginCount());
            } else {
                System.out.println("Rejected Invalid Event: " + event.getClass().getSimpleName());
            }
        }
    }

    private static final Queue<EventBase> eventQueue = new LinkedList<>();
    private static final FinancialTracker tracker = new FinancialTracker();
    private static final PipelineRegistry pipeline = new PipelineRegistry();

    static {
        // Wire up the pipeline relationships using clean, modern lambda factories
        pipeline.register(LoginEvent.class, new ComponentFactory<LoginEvent, LoginValidator, LoginProcessor>() {
            @Override public LoginValidator createValidator(LoginEvent ev) { return new LoginValidator(ev); }
            @Override public LoginProcessor createProcessor(FinancialTracker trk) { return new LoginProcessor(trk); }
        });

        pipeline.register(PurchaseEvent.class, new ComponentFactory<PurchaseEvent, PurchaseValidator, PurchaseProcessor>() {
            @Override public PurchaseValidator createValidator(PurchaseEvent ev) { return new PurchaseValidator(ev); }
            @Override public PurchaseProcessor createProcessor(FinancialTracker trk) { return new PurchaseProcessor(trk); }
        });

        pipeline.register(RefundEvent.class, new ComponentFactory<RefundEvent, RefundValidator, RefundProcessor>() {
            @Override public RefundValidator createValidator(RefundEvent ev) { return new RefundValidator(ev); }
            @Override public RefundProcessor createProcessor(FinancialTracker trk) { return new RefundProcessor(trk); }
        });
    }
    
    public static void main(String[] args) {

        eventQueue.add(new LoginEvent(101));
        eventQueue.add(new PurchaseEvent(250.50));
        eventQueue.add(new RefundEvent(50.25));
        eventQueue.add(new PurchaseEvent(-10)); // Invalid data point
        eventQueue.add(new LoginEvent(102));

        // Your code here
        RunETLLoop();
    }
}
