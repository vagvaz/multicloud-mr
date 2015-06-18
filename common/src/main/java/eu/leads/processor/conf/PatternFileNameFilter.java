package eu.leads.processor.conf;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Created by vagvaz on 6/1/14.
 */
public class PatternFileNameFilter implements FilenameFilter {
    private String pattern;

    public PatternFileNameFilter(String pattern) {
        this.pattern = pattern;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(File dir, String name) {
        return name.matches(pattern);
    }
}
