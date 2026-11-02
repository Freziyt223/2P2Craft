package general.p2craft.p2craft;

import net.fabricmc.api.ClientModInitializer;

public class P2Craft implements ClientModInitializer {
    
    @Override
    public void onInitializeClient() {
        
        // This method is invoked by the Fabric mod loader when it is ready
        // to load your mod. You can access Fabric and Common code in this
        // project.

        // Use Fabric to bootstrap the Common mod.
        Constants.LOG.info("Hello Fabric world!");
        CommonClass.init();
    }
}
