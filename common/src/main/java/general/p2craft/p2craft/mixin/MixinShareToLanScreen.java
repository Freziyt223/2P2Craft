package general.p2craft.p2craft.mixin;

import general.p2craft.p2craft.gui.P2CraftScreen;
import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.components.AbstractWidget;
import net.minecraft.client.gui.components.Button;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.client.gui.screens.ShareToLanScreen;
import net.minecraft.network.chat.Component;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;

@Mixin(ShareToLanScreen.class)
public abstract class MixinShareToLanScreen extends Screen {
    protected MixinShareToLanScreen(Component title) {
        super(title);
    }

    @Inject(method = "init", at = @At("TAIL"))
    private void p2craft$addMenuButton(CallbackInfo ci) {
        int buttonWidth = Math.min(150, width - 20);
        int x = (width - buttonWidth) / 2;
        int lowestWidgetY = -1;
        for (var child : children()) {
            if (child instanceof AbstractWidget widget) {
                lowestWidgetY = Math.max(lowestWidgetY, widget.getY());
            }
        }
        // ShareToLanScreen puts its Start button at the bottom; place ours just above it.
        int y = lowestWidgetY < 0 ? height / 2 : Math.max(0, lowestWidgetY - 24);
        addRenderableWidget(Button.builder(Component.literal("2P2Craft"), button ->
            Minecraft.getInstance().setScreen(new P2CraftScreen((Screen) (Object) this)))
            .bounds(x, y, buttonWidth, 20).build());
    }
}
