package com.att.h2o;

import water.bindings.pojos.H2OModelBuilderErrorV3;
import water.bindings.pojos.ModelParametersSchemaV3;
import water.bindings.pojos.ValidationMessageV3;

public class H2OModelBuilderException extends H2OException {
    private ValidationMessageV3[] validationMessages;
    private int errorCount;
    private ModelParametersSchemaV3 parameters;

    public H2OModelBuilderException(H2OModelBuilderErrorV3 error) {
        super(error);
        this.validationMessages = error.messages;
        this.errorCount = error.errorCount;
        this.parameters = error.parameters;
    }

    public H2OModelBuilderException(ValidationMessageV3[] validationMessages, int errorCount, ModelParametersSchemaV3 parameters) {
        super("Illegal argument(s) for model: " + validationMessages[0].message);
        this.validationMessages = validationMessages;
        this.errorCount = errorCount;
        this.parameters = parameters;
    }

    public ValidationMessageV3[] getValidationMessages() {
        return validationMessages;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public ModelParametersSchemaV3 getParameters() {
        return parameters;
    }
}
