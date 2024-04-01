package com.subscribe.nifi;

import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;

public abstract class AbstractRedisSubscribe extends AbstractSessionFactoryProcessor {
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory)
            throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, session);
            session.commit();
        } catch (ProcessException t) {
            session.rollback(true);
            throw t;
        } catch (IOException e) {
            session.rollback(true);
            throw new ProcessException(e);
        }
    }

    public abstract void onTrigger(ProcessContext context, ProcessSession session)
            throws ProcessException, IOException;
}
