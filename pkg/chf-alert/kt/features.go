package kt

// A single place where we can list the feature flags currently in use in the app, and set global enable/disable defaults

const (
	FeatureNotificationsDebugging               = "streaming.notifications.debugging"
	FeatureRTBHSync                             = "streaming.mitigate2.rtbhsync"
	FeatureFlowspecSync                         = "streaming.mitigate2.flowspecsync"
	FeatureDumpParserOutput                     = "streaming.agg.parser.dump.output"
	FeatureRadwareRecentAttackStopQuiesce       = "streaming.mitigate.radware.recentattackstop.quiesce"
	FeatureRadwareRecentAttackStopNotMitigating = "streaming.mitigate.radware.recentattackstop.notmitigating"
	FeatureMitigateExtendedKey                  = "streaming.mitigate.extendedkey"
	FeatureMitigateAutoStopEscalate             = "streaming.mitigate.autostopescalate"

	FeatureActivateHardReload = "streaming.activate.hardreload"

	FeatureBaselineFrontfill              = "streaming.baseline.frontfill"
	FeatureBaselineFrontfillPersistSlab   = "streaming.baseline.frontfill.slab.persist"
	FeatureBaselineFrontfillLegacyUpload  = "streaming.baseline.frontfill.legacy.upload"
	FeatureBaselineActivateLegacyUpload   = "streaming.baseline.activate.legacy.upload"
	FeatureBaselineActivateLegacyDownload = "streaming.baseline.activate.legacy.download"
	FeatureAggIPSetTest                   = "streaming.agg.ipset.test"

	FeatureConductorClient = "streaming.conductor.client"

	FeatureAggParallel = "streaming.agg.parallel"
)

var (
	FeaturesEnabledByDefault = []string{
		FeatureConductorClient,
		FeatureFlowspecSync,
		FeatureRTBHSync,
		FeatureRadwareRecentAttackStopQuiesce,

		FeatureMitigateAutoStopEscalate,

		FeatureBaselineFrontfill,            // frontfill for all
		FeatureBaselineFrontfillPersistSlab, // baseline local persistence
	}
)
