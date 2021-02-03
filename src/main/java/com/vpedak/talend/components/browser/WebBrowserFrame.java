package com.vpedak.talend.components.browser;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.KeyboardFocusManager;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.swing.JFrame;

import org.cef.CefApp;
import org.cef.CefApp.CefAppState;
import org.cef.CefClient;
import org.cef.CefSettings;
import org.cef.CefSettings.LogSeverity;
import org.cef.browser.CefBrowser;
import org.cef.browser.CefFrame;
import org.cef.handler.CefAppHandlerAdapter;
import org.cef.handler.CefDisplayHandler;
import org.cef.handler.CefFocusHandlerAdapter;

/**
 * This is a simple example application using JCEF.
 * It displays a JFrame with a JTextField at its top and a CefBrowser in its
 * center. The JTextField is used to enter and assign an URL to the browser UI.
 * No additional handlers or callbacks are used in this example.
 *
 * The number of used JCEF classes is reduced (nearly) to its minimum and should
 * assist you to get familiar with JCEF.
 *
 * For a more feature complete example have also a look onto the example code
 * within the package "tests.detailed".
 */
public class WebBrowserFrame extends JFrame {
    private static final long serialVersionUID = -5570653778104813836L;
    private final CefApp cefApp_;
    private final CefClient client_;
    private final CefBrowser browser_;
    private final Component browerUI_;
    private boolean browserFocus_ = true;
    private boolean data = false;
    private List<String> list;
    private CountDownLatch latch;

    /**
     * To display a simple browser window, it suffices completely to create an
     * instance of the class CefBrowser and to assign its UI component to your
     * application (e.g. to your content pane).
     * But to be more verbose, this CTOR keeps an instance of each object on the
     * way to the browser UI.
     * @param latch 
     */
    private WebBrowserFrame(String startURL, List<String> list, CountDownLatch latch) {
    	this.list = list;
    	this.latch = latch;
    	
        // (1) The entry point to JCEF is always the class CefApp. There is only one
        //     instance per application and therefore you have to call the method
        //     "getInstance()" instead of a CTOR.
        //
        //     CefApp is responsible for the global CEF context. It loads all
        //     required native libraries, initializes CEF accordingly, starts a
        //     background task to handle CEF's message loop and takes care of
        //     shutting down CEF after disposing it.
        CefApp.addAppHandler(new CefAppHandlerAdapter(null) {
            @Override
            public void stateHasChanged(org.cef.CefApp.CefAppState state) {
                // Shutdown the app if the native CEF part is terminated
                if (state == CefAppState.TERMINATED) System.exit(0);
            }
        });
        CefSettings settings = new CefSettings();
        settings.windowless_rendering_enabled = false;
        cefApp_ = CefApp.getInstance(settings);

        // (2) JCEF can handle one to many browser instances simultaneous. These
        //     browser instances are logically grouped together by an instance of
        //     the class CefClient. In your application you can create one to many
        //     instances of CefClient with one to many CefBrowser instances per
        //     client. To get an instance of CefClient you have to use the method
        //     "createClient()" of your CefApp instance. Calling an CTOR of
        //     CefClient is not supported.
        //
        //     CefClient is a connector to all possible events which come from the
        //     CefBrowser instances. Those events could be simple things like the
        //     change of the browser title or more complex ones like context menu
        //     events. By assigning handlers to CefClient you can control the
        //     behavior of the browser. See tests.detailed.MainFrame for an example
        //     of how to use these handlers.
        client_ = cefApp_.createClient();

        // (3) One CefBrowser instance is responsible to control what you'll see on
        //     the UI component of the instance. It can be displayed off-screen
        //     rendered or windowed rendered. To get an instance of CefBrowser you
        //     have to call the method "createBrowser()" of your CefClient
        //     instances.
        //
        //     CefBrowser has methods like "goBack()", "goForward()", "loadURL()",
        //     and many more which are used to control the behavior of the displayed
        //     content. The UI is held within a UI-Compontent which can be accessed
        //     by calling the method "getUIComponent()" on the instance of CefBrowser.
        //     The UI component is inherited from a java.awt.Component and therefore
        //     it can be embedded into any AWT UI.
        browser_ = client_.createBrowser(startURL, false, false);
        browerUI_ = browser_.getUIComponent();

        // Clear focus from the address field when the browser gains focus.
        client_.addFocusHandler(new CefFocusHandlerAdapter() {
            @Override
            public void onGotFocus(CefBrowser browser) {
                if (browserFocus_) return;
                browserFocus_ = true;
                KeyboardFocusManager.getCurrentKeyboardFocusManager().clearGlobalFocusOwner();
                browser.setFocus(true);
            }

            @Override
            public void onTakeFocus(CefBrowser browser, boolean next) {
                browserFocus_ = false;
            }
        });
        
        client_.addDisplayHandler(new CefDisplayHandler() {
			@Override
			public boolean onTooltip(CefBrowser var1, String var2) {
				return false;
			}
			@Override
			public void onTitleChange(CefBrowser var1, String var2) {
			}
			@Override
			public void onStatusMessage(CefBrowser var1, String var2) {
			}
			@Override
			public boolean onConsoleMessage(CefBrowser browser, LogSeverity level, String message, String source, int line) {
				if (message.equals("#@SELECTED_ITEMS@#")) {
					data = true;
				} else if (data) {
					if (message.equals("#@END@#")) {
						data = false;
						
		                latch.countDown();
					} else {
						list.add(message);
					}
				}
				return true;
			}
			
			@Override
			public void onAddressChange(CefBrowser var1, CefFrame var2, String var3) {
			}
		});
        
        getContentPane().add(browerUI_, BorderLayout.CENTER);
        pack();
        setSize(800, 600);

        // setExtendedState(JFrame.MAXIMIZED_BOTH); 
        // setUndecorated(true);
        
        
        setVisible(true);

        // (6) To take care of shutting down CEF accordingly, it's important to call
        //     the method "dispose()" of the CefApp instance if the Java
        //     application will be closed. Otherwise you'll get asserts from CEF.
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                CefApp.getInstance().dispose();
                dispose();
                latch.countDown();
            }
        });
    }

    public static void start(String url, List<String> list) {
        // Perform startup initialization on platforms that require it.
        if (!CefApp.startup()) {
            System.out.println("Startup initialization failed!");
            return;
        }

        CountDownLatch latch = new CountDownLatch(1);        
        new WebBrowserFrame(url, list, latch);
        try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
}
