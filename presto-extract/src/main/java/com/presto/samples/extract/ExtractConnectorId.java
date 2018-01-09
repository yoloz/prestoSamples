package com.presto.samples.extract;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Created by ethan
 * 17-10-18
 */
public final class ExtractConnectorId {

    private final String id;

    public ExtractConnectorId(String id) {
        this.id = requireNonNull(id, "id is null");
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ExtractConnectorId other = (ExtractConnectorId) obj;
        return Objects.equals(this.id, other.id);
    }
}
