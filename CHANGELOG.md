# Changelog

## [0.43.0](https://github.com/praveen2450/datacloud-jdbc/compare/v0.42.1...v0.43.0) (2026-02-16)


### Features

* add configuration for channel keepalive and retry ([#48](https://github.com/praveen2450/datacloud-jdbc/issues/48)) ([5067644](https://github.com/praveen2450/datacloud-jdbc/commit/50676443bab7528b6028d766acd44395858f1402))
* Add Float4Vector support - Add FloatVectorAccessor, tests, and wire up in factory ([#46](https://github.com/praveen2450/datacloud-jdbc/issues/46)) ([7316400](https://github.com/praveen2450/datacloud-jdbc/commit/73164005772e71b47e7551dd60faf4f9adf308e2))
* add getQueryInfo retry mechanism for usage in FSMs ([#91](https://github.com/praveen2450/datacloud-jdbc/issues/91)) ([4cdcc5e](https://github.com/praveen2450/datacloud-jdbc/commit/4cdcc5e9d9d42a358eb30586eb9730fb05830408))
* add http logging and grpc tracing from oauth driver ([#49](https://github.com/praveen2450/datacloud-jdbc/issues/49)) ([dff10a6](https://github.com/praveen2450/datacloud-jdbc/commit/dff10a6be48af2ed3d814fd7ce7f10d640ecf5f3))
* Add integration test as mandatory gate before publishing ([#123](https://github.com/praveen2450/datacloud-jdbc/issues/123)) ([c67751b](https://github.com/praveen2450/datacloud-jdbc/commit/c67751b6816a952dfd6a014a2c5738ef40af140a))
* Add result set constraints and chunk-based polling ([#47](https://github.com/praveen2450/datacloud-jdbc/issues/47)) ([d2ec067](https://github.com/praveen2450/datacloud-jdbc/commit/d2ec067264878494ffb4bb5100e7fb3e63202a3f))
* add row-based access of results to connection ([#18](https://github.com/praveen2450/datacloud-jdbc/issues/18)) ([1471135](https://github.com/praveen2450/datacloud-jdbc/commit/14711351cdafe788b4fa2a5640988322df0e4494))
* Add SSL/TLS Support to DataCloud JDBC Driver ([#89](https://github.com/praveen2450/datacloud-jdbc/issues/89)) ([9123c1b](https://github.com/praveen2450/datacloud-jdbc/commit/9123c1b8981f425a9477f5a219d5472fe7f34213))
* Add support for per JDBC Connection interceptors in combination with cached channels ([#75](https://github.com/praveen2450/datacloud-jdbc/issues/75)) ([4a52243](https://github.com/praveen2450/datacloud-jdbc/commit/4a5224329418545d49cfa0cb55f1c59042c258e1))
* adds DataCloudConnection::getSchemaForQueryId ([#113](https://github.com/praveen2450/datacloud-jdbc/issues/113)) ([b382937](https://github.com/praveen2450/datacloud-jdbc/commit/b382937ae63b80efcdfa9a70d7bab323070a81e5))
* Allow to toggle the inclusion of query text details in exception messages ([#126](https://github.com/praveen2450/datacloud-jdbc/issues/126)) ([28511e9](https://github.com/praveen2450/datacloud-jdbc/commit/28511e9df0540f98daa1add5174ee69b10041d2e))
* Downgrade library compatibility from Java 11 to Java 8 ([#4](https://github.com/praveen2450/datacloud-jdbc/issues/4)) ([90d9bbb](https://github.com/praveen2450/datacloud-jdbc/commit/90d9bbbbcbaaa34375a10fb4a7fa10d544ab133c))
* expose polling for a query status by arbitrary predicate ([#66](https://github.com/praveen2450/datacloud-jdbc/issues/66)) ([9af3dea](https://github.com/praveen2450/datacloud-jdbc/commit/9af3dea737c1c4cf1a290e2c998276194e4988fa))
* implement automated release pipeline using Release Please ([#139](https://github.com/praveen2450/datacloud-jdbc/issues/139)) ([268eb2e](https://github.com/praveen2450/datacloud-jdbc/commit/268eb2eaf7979587e919a02503786ed0c06fe41b))
* Implement basic Spark Data Source ([#54](https://github.com/praveen2450/datacloud-jdbc/issues/54)) ([a50fa7c](https://github.com/praveen2450/datacloud-jdbc/commit/a50fa7cdb10f600aa47112dc787b5aec013e41e1))
* Improved query timeout handling ([#80](https://github.com/praveen2450/datacloud-jdbc/issues/80)) ([1b82509](https://github.com/praveen2450/datacloud-jdbc/commit/1b825091bbc9f892fa65e4a380de34441d2dff8b))
* Provide a low level Async Interface ([#150](https://github.com/praveen2450/datacloud-jdbc/issues/150)) ([b1fba90](https://github.com/praveen2450/datacloud-jdbc/commit/b1fba90fdf56db4414f1a289ddf3419ce819d6b4))
* remove DBeaver workaround and simplify JAR configuration ([#111](https://github.com/praveen2450/datacloud-jdbc/issues/111)) ([4c4a695](https://github.com/praveen2450/datacloud-jdbc/commit/4c4a6952121baecd496b1a23521ef7b8f6690c9d))
* Revamp connection string and property handling ([#112](https://github.com/praveen2450/datacloud-jdbc/issues/112)) ([3417a45](https://github.com/praveen2450/datacloud-jdbc/commit/3417a454b43c80f91ff6e2945796a9cc1089ed56))
* separate spark-datasource and spark-datasource-core ([#94](https://github.com/praveen2450/datacloud-jdbc/issues/94)) ([0422a05](https://github.com/praveen2450/datacloud-jdbc/commit/0422a0555ff3a00412b612856312ed2cb123688c))
* Support PreparedStatement.getMetaData() ([#151](https://github.com/praveen2450/datacloud-jdbc/issues/151)) ([52448d9](https://github.com/praveen2450/datacloud-jdbc/commit/52448d9ee0c82d17c70af651eb8282658a38ead0))


### Bug Fixes

* add missing protobuf-java library and add flattenJars task ([#50](https://github.com/praveen2450/datacloud-jdbc/issues/50)) ([ca2f8d8](https://github.com/praveen2450/datacloud-jdbc/commit/ca2f8d8590e579ce25620c5980c1ce52ca992048))
* apply refactor &gt; delombok to remove @UtilityClass ([#73](https://github.com/praveen2450/datacloud-jdbc/issues/73)) ([4bbb96d](https://github.com/praveen2450/datacloud-jdbc/commit/4bbb96dfb24015c2678e1641f6e141bc131157a2))
* Breaking - Remove data loss for slow readers ([#142](https://github.com/praveen2450/datacloud-jdbc/issues/142)) ([1ff41dc](https://github.com/praveen2450/datacloud-jdbc/commit/1ff41dc64c5619d76eeb7cb5e51a2f6d91c6f38d))
* **ci:** remove explicit SNAPSHOT version from snapshot workflow ([#141](https://github.com/praveen2450/datacloud-jdbc/issues/141)) ([b30c3fe](https://github.com/praveen2450/datacloud-jdbc/commit/b30c3fec1e1db2ea189e582774303ebe8bb8b5c1))
* **ci:** synchronize Release Please state to resolve empty change set error ([#143](https://github.com/praveen2450/datacloud-jdbc/issues/143)) ([8518b23](https://github.com/praveen2450/datacloud-jdbc/commit/8518b23a07ff27e15ea0900e330303d57705718a))
* **ci:** use simple release-type with extra-files for Gradle project ([#145](https://github.com/praveen2450/datacloud-jdbc/issues/145)) ([5a8aac4](https://github.com/praveen2450/datacloud-jdbc/commit/5a8aac455df8668964ce38739e1865363884777a))
* enforce protobuf-java version with bom ([#51](https://github.com/praveen2450/datacloud-jdbc/issues/51)) ([2535867](https://github.com/praveen2450/datacloud-jdbc/commit/25358673db6314d4423ef76901e3432397de5dbf))
* ensure user agent is applied to gRPC channel ([#2](https://github.com/praveen2450/datacloud-jdbc/issues/2)) ([259351c](https://github.com/praveen2450/datacloud-jdbc/commit/259351c5e7a03d394a9a162eff928888f65b58fd))
* force MetadataResultSet to be forward only ([#61](https://github.com/praveen2450/datacloud-jdbc/issues/61)) ([aa45a97](https://github.com/praveen2450/datacloud-jdbc/commit/aa45a97d31da1760aca043db87585634cb1de2f5))
* Gracefully handle large incoming headers ([#125](https://github.com/praveen2450/datacloud-jdbc/issues/125)) ([b2f9932](https://github.com/praveen2450/datacloud-jdbc/commit/b2f99324b1bc72261c30905e63cd0ec2d8f65aa8))
* Incorrect date-time reporting in the UI due to time zone handling ([#9](https://github.com/praveen2450/datacloud-jdbc/issues/9)) ([fcb4c6c](https://github.com/praveen2450/datacloud-jdbc/commit/fcb4c6cccf3c9d372a5bdc16ce81c0a417451fb6))
* loosen audience requirements for jwt creation to match docs ([#64](https://github.com/praveen2450/datacloud-jdbc/issues/64)) ([a8fe55c](https://github.com/praveen2450/datacloud-jdbc/commit/a8fe55c7d2659dbbb850cd50eb3f6bfb06650860))
* orphan managed channel error in HyperResultSourceTest ([#98](https://github.com/praveen2450/datacloud-jdbc/issues/98)) ([83a94fb](https://github.com/praveen2450/datacloud-jdbc/commit/83a94fb9d6233588cdf5ce441dcf336ca6ecf019))
* **perf:** make our ReadableByteChannel more efficient by removing toByteArray ([#87](https://github.com/praveen2450/datacloud-jdbc/issues/87)) ([114f372](https://github.com/praveen2450/datacloud-jdbc/commit/114f372c638678ff70f9eeafc46fedf1f0badd01))
* pin JReleaser to version 1.19.0 to resolve GPG signing issues ([#120](https://github.com/praveen2450/datacloud-jdbc/issues/120)) ([6dd1615](https://github.com/praveen2450/datacloud-jdbc/commit/6dd161596baad1b12d1ed061c4bc2a5e6f78755b))
* prevent subsequent getQueryInfo on small query ([#41](https://github.com/praveen2450/datacloud-jdbc/issues/41)) ([519f771](https://github.com/praveen2450/datacloud-jdbc/commit/519f771932ded61ec33ff0270d7d656dfb77c29c))
* Remove comments from JDBC driver version ([#153](https://github.com/praveen2450/datacloud-jdbc/issues/153)) ([32f7216](https://github.com/praveen2450/datacloud-jdbc/commit/32f7216bac41cdf6c2621d079f8c8fd77cd49a16))
* Remove mandatory clientSecret when using PrivateKey/JWT and fix JWT "audience is invalid" error ([#132](https://github.com/praveen2450/datacloud-jdbc/issues/132)) ([f6d3757](https://github.com/praveen2450/datacloud-jdbc/commit/f6d3757cee503b1cf849529c76d005f8306235d9))
* Remove redundant header grpc.max_metadata_size ([#14](https://github.com/praveen2450/datacloud-jdbc/issues/14)) ([a229637](https://github.com/praveen2450/datacloud-jdbc/commit/a229637101ed5148dcc9e0c6de96ec4f740f122d))
* Remove required dataspace parameter from Connection ([#133](https://github.com/praveen2450/datacloud-jdbc/issues/133)) ([cc53def](https://github.com/praveen2450/datacloud-jdbc/commit/cc53def4ceea631e5315e3d49b6e5d19676ca22d))
* replace OOM succeptible Stream API based Adaptive query client with Finite State Machine implementation ([#85](https://github.com/praveen2450/datacloud-jdbc/issues/85)) ([71d31e9](https://github.com/praveen2450/datacloud-jdbc/commit/71d31e978ac9d08811503bfc74cbf9695ddebfa2))
* resolve DataCloudArray data loss after ResultSet exhaustion ([#115](https://github.com/praveen2450/datacloud-jdbc/issues/115)) ([bcc2fcd](https://github.com/praveen2450/datacloud-jdbc/commit/bcc2fcd736158b169e4e5ce9b411fd9bdbd73bb7))
* resolve gRPC NameResolver service file merging in shaded JAR ([#122](https://github.com/praveen2450/datacloud-jdbc/issues/122)) ([abad390](https://github.com/praveen2450/datacloud-jdbc/commit/abad39001acd1e11cfb0277c31d329da307dfcb0))
* resolve Maven versioning issue ([#20](https://github.com/praveen2450/datacloud-jdbc/issues/20)) ([67ae9c5](https://github.com/praveen2450/datacloud-jdbc/commit/67ae9c5da53460467e936446642d73aa5458c645))
* resolve release workflow validation error for tag expression ([#156](https://github.com/praveen2450/datacloud-jdbc/issues/156)) ([449ce0d](https://github.com/praveen2450/datacloud-jdbc/commit/449ce0d5ba517e42f6f49eaaaeafcb1712b286b8))
* Restrict Avatica to version before 1.27.0 to make the driver buildable again ([#117](https://github.com/praveen2450/datacloud-jdbc/issues/117)) ([331ca5a](https://github.com/praveen2450/datacloud-jdbc/commit/331ca5ac06c4a3249f83f3389332b455a8b72dd0))
* Switch build back to Gradle 8 ([#116](https://github.com/praveen2450/datacloud-jdbc/issues/116)) ([e35505d](https://github.com/praveen2450/datacloud-jdbc/commit/e35505d1e2a130fea4d71c142b9e7ab3ba3d2aa4))
* Truncate query in exceptions and add test coverage for protocol limits ([#15](https://github.com/praveen2450/datacloud-jdbc/issues/15)) ([e11d03e](https://github.com/praveen2450/datacloud-jdbc/commit/e11d03eaacf1548322fda4beb7731ded25678bd2))
* Update release workflow to Java 8 ([#5](https://github.com/praveen2450/datacloud-jdbc/issues/5)) ([96ea6a2](https://github.com/praveen2450/datacloud-jdbc/commit/96ea6a20e1d4b7848e911e17a6822ae77ea13ca8))
* update version to 0.35.0 and resolves 409 conflict error in snapshot builds ([#119](https://github.com/praveen2450/datacloud-jdbc/issues/119)) ([2917578](https://github.com/praveen2450/datacloud-jdbc/commit/2917578b7f9a725c594e3c55ec71e238899e0e76))
* using channel builder in DataCloudConnection should always cleanup ([#53](https://github.com/praveen2450/datacloud-jdbc/issues/53)) ([00015d5](https://github.com/praveen2450/datacloud-jdbc/commit/00015d57c7edc4f8c61e97b89233f09250da8ab6))
* Validate JDBC settings properties and throw user errors ([#107](https://github.com/praveen2450/datacloud-jdbc/issues/107)) ([dd87564](https://github.com/praveen2450/datacloud-jdbc/commit/dd8756446d7637d4ba5b827f7ee7637bd1377bf6))
* withDeadlineAfter Duration overload not available in older grpc ([#62](https://github.com/praveen2450/datacloud-jdbc/issues/62)) ([f0e03ea](https://github.com/praveen2450/datacloud-jdbc/commit/f0e03eab647687b1aca067b111e7ce105f9ebb1a))


### Performance Improvements

* Optimize ResultSet column lookup with HashMap-based indexing ([#138](https://github.com/praveen2450/datacloud-jdbc/issues/138)) ([b8c5eb9](https://github.com/praveen2450/datacloud-jdbc/commit/b8c5eb96f3cf57bb809d947f5d45cd32f15bfe79))

## [0.42.1](https://github.com/forcedotcom/datacloud-jdbc/compare/v0.42.0...v0.42.1) (2026-02-06)


### Bug Fixes

* Remove comments from JDBC driver version ([#153](https://github.com/forcedotcom/datacloud-jdbc/issues/153)) ([32f7216](https://github.com/forcedotcom/datacloud-jdbc/commit/32f7216bac41cdf6c2621d079f8c8fd77cd49a16))
* resolve release workflow validation error for tag expression ([#156](https://github.com/forcedotcom/datacloud-jdbc/issues/156)) ([449ce0d](https://github.com/forcedotcom/datacloud-jdbc/commit/449ce0d5ba517e42f6f49eaaaeafcb1712b286b8))

## [0.42.0](https://github.com/forcedotcom/datacloud-jdbc/compare/0.41.0...v0.42.0) (2026-02-05)


### Features

* Add SSL/TLS Support to DataCloud JDBC Driver ([#89](https://github.com/forcedotcom/datacloud-jdbc/issues/89)) ([9123c1b](https://github.com/forcedotcom/datacloud-jdbc/commit/9123c1b8981f425a9477f5a219d5472fe7f34213))
* implement automated release pipeline using Release Please ([#139](https://github.com/forcedotcom/datacloud-jdbc/issues/139)) ([268eb2e](https://github.com/forcedotcom/datacloud-jdbc/commit/268eb2eaf7979587e919a02503786ed0c06fe41b))
* Provide a low level Async Interface ([#150](https://github.com/forcedotcom/datacloud-jdbc/issues/150)) ([b1fba90](https://github.com/forcedotcom/datacloud-jdbc/commit/b1fba90fdf56db4414f1a289ddf3419ce819d6b4))
* Support PreparedStatement.getMetaData() ([#151](https://github.com/forcedotcom/datacloud-jdbc/issues/151)) ([52448d9](https://github.com/forcedotcom/datacloud-jdbc/commit/52448d9ee0c82d17c70af651eb8282658a38ead0))


### Bug Fixes

* Breaking - Remove data loss for slow readers ([#142](https://github.com/forcedotcom/datacloud-jdbc/issues/142)) ([1ff41dc](https://github.com/forcedotcom/datacloud-jdbc/commit/1ff41dc64c5619d76eeb7cb5e51a2f6d91c6f38d))
* **ci:** remove explicit SNAPSHOT version from snapshot workflow ([#141](https://github.com/forcedotcom/datacloud-jdbc/issues/141)) ([b30c3fe](https://github.com/forcedotcom/datacloud-jdbc/commit/b30c3fec1e1db2ea189e582774303ebe8bb8b5c1))
* **ci:** synchronize Release Please state to resolve empty change set error ([#143](https://github.com/forcedotcom/datacloud-jdbc/issues/143)) ([8518b23](https://github.com/forcedotcom/datacloud-jdbc/commit/8518b23a07ff27e15ea0900e330303d57705718a))
* **ci:** use simple release-type with extra-files for Gradle project ([#145](https://github.com/forcedotcom/datacloud-jdbc/issues/145)) ([5a8aac4](https://github.com/forcedotcom/datacloud-jdbc/commit/5a8aac455df8668964ce38739e1865363884777a))
* Remove comments from JDBC driver version ([#153](https://github.com/forcedotcom/datacloud-jdbc/issues/153)) ([32f7216](https://github.com/forcedotcom/datacloud-jdbc/commit/32f7216bac41cdf6c2621d079f8c8fd77cd49a16))


### Performance Improvements

* Optimize ResultSet column lookup with HashMap-based indexing ([#138](https://github.com/forcedotcom/datacloud-jdbc/issues/138)) ([b8c5eb9](https://github.com/forcedotcom/datacloud-jdbc/commit/b8c5eb96f3cf57bb809d947f5d45cd32f15bfe79))

## Changelog
