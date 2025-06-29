
# Find help2man
find_program(HELP2MAN help2man DOC "Path to help2man executable")

include(GNUInstallDirs)

if(NOT HELP2MAN)
    message(WARNING "help2man not found - man pages will not be generated")
else()
    message(STATUS "Found help2man: ${HELP2MAN}")

    # Set the output directory for man pages
    set(MAN_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}")
    file(MAKE_DIRECTORY ${MAN_OUTPUT_DIR})

    # cvmfs2
    add_custom_command(
        OUTPUT "${MAN_OUTPUT_DIR}/cvmfs2.1"
        COMMAND ${HELP2MAN} 
            -i ${CMAKE_CURRENT_LIST_DIR}/cvmfs_common.h2m
            -i ${CMAKE_CURRENT_LIST_DIR}/cvmfs2.h2m
            --no-info 
            --section=1 
            --output=${MAN_OUTPUT_DIR}/cvmfs2.1
            --name "cvmfs2: fuse client for the CernVM-Filesystem" 
            "$<TARGET_FILE:cvmfs2>"
        DEPENDS cvmfs2
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/cvmfs/
        COMMENT "Generating man page for cvmfs2"
        VERBATIM
    )

    # Add to the list of generated man pages
    list(APPEND MAN_PAGES "${MAN_OUTPUT_DIR}/cvmfs2.1")
    install(
        FILES ${MAN_OUTPUT_DIR}/cvmfs2.1
        #TYPE MAN
        DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
        COMPONENT documentation
    )
    # cvmfs_config
    add_custom_command(
        OUTPUT "${MAN_OUTPUT_DIR}/cvmfs_config.1"
        COMMAND ${HELP2MAN} 
            -i ${CMAKE_CURRENT_LIST_DIR}/cvmfs_common.h2m
            --no-info 
            --section=1 
            --output=${MAN_OUTPUT_DIR}/cvmfs_config.1
            --name "cvmfs_config: tools for common CernVM-FS config tasks" 
            "${CMAKE_SOURCE_DIR}/cvmfs/cvmfs_config"
        COMMENT "Generating man page for cvmfs_config"
        VERBATIM
    )

    # Add to the list of generated man pages
    list(APPEND MAN_PAGES "${MAN_OUTPUT_DIR}/cvmfs_config.1")
    install(
        FILES ${MAN_OUTPUT_DIR}/cvmfs_config.1
        #TYPE MAN doesn't install into correct section dir
        DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
        COMPONENT documentation
    )
    if (BUILD_SERVER) 

    # cvmfs_server
    add_custom_command(
        OUTPUT "${MAN_OUTPUT_DIR}/cvmfs_server.1"
        COMMAND ${CMAKE_CURRENT_LIST_DIR}/generate_manpage_cvmfs_server.sh
                ${CMAKE_CURRENT_LIST_DIR} # location of include files
                ${MAN_OUTPUT_DIR} # directory for generated manpage
                ${CMAKE_BINARY_DIR}/cvmfs # location of swissknife binary
        DEPENDS cvmfs_server_script
        COMMENT "Generating man page for cvmfs_server"
        VERBATIM
    )
    # Add to the list of generated man pages
    list(APPEND MAN_PAGES "${MAN_OUTPUT_DIR}/cvmfs_server.1")
    install(
        FILES ${MAN_OUTPUT_DIR}/cvmfs_server.1
        #TYPE MAN doesn't install into correct section dir
        DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
        COMPONENT documentation
    )
    # cvmfs_swissknife
    add_custom_command(
        OUTPUT "${MAN_OUTPUT_DIR}/cvmfs_swissknife.1"
        COMMAND ${CMAKE_CURRENT_LIST_DIR}/generate_manpage_cvmfs_swissknife.sh
                ${CMAKE_CURRENT_LIST_DIR} # location of include files
                ${MAN_OUTPUT_DIR} # directory for generated manpage
                ${CMAKE_BINARY_DIR}/cvmfs # location of swissknife binary
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/cvmfs
        DEPENDS cvmfs_swissknife
        COMMENT "Generating man page for cvmfs_swissknife"
        VERBATIM
    )
    # Add to the list of generated man pages
    list(APPEND MAN_PAGES "${MAN_OUTPUT_DIR}/cvmfs_swissknife.1")
    install(
        FILES ${MAN_OUTPUT_DIR}/cvmfs_swissknife.1
        #TYPE MAN doesn't install into correct section dir
        DESTINATION ${CMAKE_INSTALL_MANDIR}/man1
        COMPONENT documentation
    )

    endif()
    # Create a custom target for generating all man pages
    add_custom_target(manpages ALL DEPENDS ${MAN_PAGES})

endif()

