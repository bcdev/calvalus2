package com.bc.calvalus.portal.client;

/**
 * @author Marco Peters
 */
enum StageType {
    NO_STAGING {
        @Override
        String getText() {
            return "";
        }
    }, AUTO_STAGING {
        @Override
        String getText() {
            return "Auto-staging";
        }
    }, STAGE {
        @Override
        String getText() {
            return ManageProductionsView.STAGE;
        }
    }, MULTI_STAGE {
        @Override
        String getText() {
            return ManageProductionsView.STAGE;
        }
    };

    abstract String getText();
}
