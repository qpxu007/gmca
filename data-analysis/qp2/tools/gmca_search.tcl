#! /usr/bin/wish -f
#
# gmca_search.tcl -- Standalone search tool for GM/CA beamline toolbar.
# Launches alongside (or instead of) the regular toolbar.
# Usage:  wish gmca_search.tcl 23o [staff|users] [caqtdm|medm]
#
# This script sources the same config and menu-building files as gmca.tcl,
# then hides the toolbar window and presents a searchable interface to
# every menu item.
#

#-----------------------------------------------------------------------------------------
  proc pError {text} {
	puts stderr "${text}"
	exit
  }

#-----------------------------------------------------------------------------------------
proc pSetCaqtdmEnv {basedir} {
  global env SYSTEM bln
  set dirs1 [glob -nocomplain -type d -directory $basedir *App]
  lappend dirs1 ${basedir}/fastscans
  if { $bln == "23i:" || $bln == "23o:" } {
     set dirs1 [lsearch -inline -all -not $dirs1 */mtrApp]
     set dirs1 [lsearch -inline -all -not $dirs1 */pmacApp]
  }
  set dirs1 [lsearch -inline -all -not $dirs1 */gmcaApp]
  set adldirs {}
  foreach dir $dirs1 {
     lappend adldirs {*}[glob -nocomplain -type d -directory $dir "adl"]
  }
  set dirs2 $adldirs
  foreach dir $dirs2 {
     set dirs3 [glob -nocomplain -type d -directory $dir *]
     if {[llength $dirs3] > 0} {
        foreach d $dirs3 {
           set uilist [glob -nocomplain -type f -directory $d "*.ui"]
           if {[llength $uilist] > 0} {lappend adldirs $d;}
        }
     }
  }
  set adldirs [lsort -increasing $adldirs]
  set adldirs [linsert $adldirs 0 ${basedir}/gmcaApp/adl]
  if {$SYSTEM != "WIN32"} {set sep ":";} else {set sep ";";}
  set env(CAQTDM_DISPLAY_PATH) [join $adldirs $sep]
}

#-----------------------------------------------------------------------------------------
# main:

  if { "$tcl_platform(os)" == "Linux" } {set SYSTEM LINUX;} \
  else                                  {set SYSTEM WIN32;}
  if {[info exists env(PEZCA)]} {
     set topdir [regsub -all {\\} $env(PEZCA) {/}]
     set topdir [regsub {/pezca$} $topdir ""]
  } else {
     if {$SYSTEM == "LINUX"} {set topdir  "/home/gmca/epics";} \
     else                    {set topdir  "c:/gmca";}
  }
  set tclGMCA  "${topdir}/gmcaApp/tcl/"

  set Syntax "\nSyntax: gmca_search.tcl 23i|23o|23b|23d \[staff|users\] \[caqtdm|medm\]\n"
  if {$argc < 1}  {
     set txt "gmca_search: Incorrect number of command line arguments.${Syntax}"
     tk_messageBox -message $txt -type ok -icon error -title "Application Error"
     pError $txt
  }

  foreach arg $argv {
     if     {[regexp {^23[iobd]$} $arg]}                  {set Beamline $arg;} \
     elseif {$arg == "staff"  || $arg == "users"}         {set Control  $arg;} \
     elseif {$arg == "caqtdm" || $arg == "medm"}          {set DM       $arg;} \
     elseif {[regexp {^geometry=\+[0-9]+\+[0-9]+$} $arg]} {set Geometry [regsub {^geometry=} $arg ""];} \
     else   {
        set txt "gmca_search: Unknown parameter \[${arg}\] in the command line.${Syntax}"
        tk_messageBox -message $txt -type ok -icon error -title "Application Error"
        pError $txt
     }
  }

  if {! [info exists Beamline]} {
     set txt "gmca_search: Beamline is not specified in the command line.${Syntax}"
     tk_messageBox -message $txt -type ok -icon error -title "Application Error"
     pError $txt
  }

  if {! [info exists Control]}  {set Control  "users";}
  if {! [info exists DM]}       {set DM       "auto";}
  if {! [info exists Geometry]} {set Geometry +200+0;}

  # Skip staff authorization for the search tool
  set bln ${Beamline}:
  set subTitle (${Control})
  source ${tclGMCA}config.tcl
  source ${tclGMCA}config${Beamline}.tcl
  if {[file exists ${tclGMCA}config_ppmac${Beamline}.tcl]} {
     source ${tclGMCA}config_ppmac${Beamline}.tcl
     set bln_has_ppmac 1
  }

  if     {$DM == "medm"}   {set use_caqtdm 0;} \
  elseif {$DM == "caqtdm"} {set use_caqtdm 1;}
  if {$use_caqtdm == 0} {set DM "medm";} \
  else                   {set DM "caqtdm";}
  pSetCaqtdmEnv $topdir;

  set errorFile "${myhome}/gmca${Beamline}_search.err"
  set systemTime [clock seconds]
  set timestamp  [clock format $systemTime -format {%Y-%m-%d %H:%M:%S}]
  set fp [open $errorFile a+]
  puts $fp "\n=========================================================="
  puts $fp "GM/CA Search Tool started at ${timestamp}\n"
  close $fp
  set errorFile ">>&${errorFile}"

### Source the original menu-building files (creates menus in the hidden "." window)
  source ${tclGMCA}main.tcl
  source ${tclGMCA}aps.tcl
  if {[info exists bln_has_ppmac]} {source ${tclGMCA}motion_ppmac.tcl;} \
  else                             {source ${tclGMCA}motion_${Control}.tcl;}
  if {$Control == "staff"} {
     source ${tclGMCA}scan.tcl
  } else {
     if {$SYSTEM != "WIN32" && $HOST != "www"} {
        source ${tclGMCA}mx.tcl
     }
  }
  source ${tclGMCA}tools.tcl

### Hide the original toolbar window
  wm withdraw .

### Override execMedm to add -noMsg (suppress caQtDM messages window)
  proc execMedm {adl {macro {}}} {
     global medm caqtdm use_caqtdm varyFont adlSTD
     if { $use_caqtdm != "1" } {
        set dm $medm
        set sc $adl
     } else {
        set dm $caqtdm
        regsub -- {\.adl$} $adl .ui sc
     }
     if {$use_caqtdm == "1"} {set cwd [pwd]; cd "$adlSTD";}
     if {$macro != {}} {
        exec -keepnewline -- $dm -attach -noMsg $varyFont -macro ${macro} -x $sc >&/dev/null &
     } else {
        exec -keepnewline -- $dm -attach -noMsg $varyFont -x $sc >&/dev/null &
     }
     if {$use_caqtdm == "1"} {cd "$cwd";}
  }

#=========================================================================
# SEARCH INDEX BUILDER
#=========================================================================

  set searchItems {}

  proc buildSearchIndex {menuWidget breadcrumb} {
     global searchItems
     set lastIdx ""
     catch {set lastIdx [$menuWidget index last]}
     if {$lastIdx eq "" || $lastIdx eq "none"} {return}

     for {set i 0} {$i <= $lastIdx} {incr i} {
        set itemType ""
        catch {set itemType [$menuWidget type $i]}

        if {$itemType eq "command"} {
           set label ""
           catch {set label [$menuWidget entrycget $i -label]}
           set cmd ""
           catch {set cmd [$menuWidget entrycget $i -command]}
           set state ""
           catch {set state [$menuWidget entrycget $i -state]}

           # Skip disabled labels (section headers) and empty labels
           if {$label ne "" && $state ne "disabled" && $cmd ne ""} {
              set path "${breadcrumb} > ${label}"
              lappend searchItems [list $label $path $cmd]
           }

        } elseif {$itemType eq "cascade"} {
           set label ""
           catch {set label [$menuWidget entrycget $i -label]}
           set submenu ""
           catch {set submenu [$menuWidget entrycget $i -menu]}

           if {$submenu ne "" && [winfo exists $submenu]} {
              set newBreadcrumb "${breadcrumb} > ${label}"
              buildSearchIndex $submenu $newBreadcrumb
           }
        }
     }
  }

### Find all top-level menubuttons and index their menus
  foreach w [winfo children .] {
     if {[winfo class $w] eq "Menubutton"} {
        set menuName ""
        catch {set menuName [$w cget -menu]}
        if {$menuName ne "" && [winfo exists $menuName]} {
           set topName [regsub {^\.mb} [winfo name $w] ""]
           buildSearchIndex $menuName $topName
        }
     }
  }

  set nItems [llength $searchItems]
  puts "Search index built: ${nItems} items found."

#=========================================================================
# SEARCH UI
#=========================================================================

  set searchVar ""
  set filteredItems $searchItems

  toplevel .search
  wm title .search "GM/CA Search - ${Beamline} (${Control})"
  wm geometry .search 700x500${Geometry}
  wm minsize .search 400 300
  wm protocol .search WM_DELETE_WINDOW {exit}

### Title
  frame .search.top -bg SteelBlue
  label .search.top.title -text "GM/CA Tool Search" -fg White -bg SteelBlue \
        -font {Helvetica 16 bold}
  label .search.top.info -text "${Beamline} (${Control}) - ${nItems} items" \
        -fg #CCCCFF -bg SteelBlue -font {Helvetica 10}
  pack .search.top.title -side left -padx 10 -pady 5
  pack .search.top.info  -side right -padx 10 -pady 5
  pack .search.top -side top -fill x

### Search entry
  frame .search.entry
  label .search.entry.lbl -text "Search:" -font {Helvetica 12}
  entry .search.entry.ent -textvariable searchVar -font {Helvetica 14} \
        -relief sunken -bd 2
  button .search.entry.clr -text "Clear" -command {set searchVar ""; searchFilter}
  pack .search.entry.lbl -side left -padx 5
  pack .search.entry.ent -side left -fill x -expand 1 -padx 5
  pack .search.entry.clr -side right -padx 5
  pack .search.entry -side top -fill x -pady 5 -padx 5

### Results listbox with scrollbar
  frame .search.results
  listbox .search.results.list -font {Helvetica 11} -selectmode single \
          -yscrollcommand {.search.results.sb set} -activestyle underline \
          -selectbackground SteelBlue -selectforeground white
  scrollbar .search.results.sb -command {.search.results.list yview}
  pack .search.results.sb   -side right -fill y
  pack .search.results.list -side left -fill both -expand 1
  pack .search.results -side top -fill both -expand 1 -padx 5 -pady 2

### Status bar
  label .search.status -text "Type to search. Double-click or press Enter to launch." \
        -fg gray40 -anchor w -font {Helvetica 9}
  pack .search.status -side bottom -fill x -padx 5 -pady 2

#=========================================================================
# SEARCH LOGIC
#=========================================================================

  proc searchFilter {} {
     global searchVar searchItems filteredItems
     set query [string tolower [string trim $searchVar]]
     set filteredItems {}

     .search.results.list delete 0 end

     if {$query eq ""} {
        set filteredItems $searchItems
     } else {
        # Split query into words for multi-word matching
        set words [split $query " "]
        foreach item $searchItems {
           set label [lindex $item 0]
           set path  [lindex $item 1]
           set searchText [string tolower "${label} ${path}"]

           # All words must match
           set allMatch 1
           foreach word $words {
              if {$word ne "" && [string first $word $searchText] == -1} {
                 set allMatch 0
                 break
              }
           }
           if {$allMatch} {
              lappend filteredItems $item
           }
        }
     }

     foreach item $filteredItems {
        set path [lindex $item 1]
        .search.results.list insert end "${path}"
     }

     set count [llength $filteredItems]
     if {$query eq ""} {
        .search.status configure -text "${count} items. Type to filter."
     } else {
        .search.status configure -text "${count} matches for \"${searchVar}\""
     }
  }

  proc searchExecute {} {
     global filteredItems
     set sel [.search.results.list curselection]
     if {$sel eq ""} {return}
     set item [lindex $filteredItems $sel]
     set label [lindex $item 0]
     set cmd   [lindex $item 2]

     .search.status configure -text "Launching: ${label}..."
     update idletasks

     if {[catch {uplevel #0 $cmd} err]} {
        .search.status configure -text "Error: ${err}"
        puts stderr "Error launching '${label}': ${err}"
     } else {
        after 2000 {
           catch {.search.status configure -text "Type to search. Double-click or press Enter to launch."}
        }
     }
  }

### Bindings
  bind .search.entry.ent <KeyRelease> {searchFilter}
  bind .search.entry.ent <Return>     {searchExecute}
  bind .search.entry.ent <Escape>     {set searchVar ""; searchFilter}
  bind .search.results.list <Double-1> {searchExecute}
  bind .search.results.list <Return>   {searchExecute}

### Focus the search entry
  focus .search.entry.ent

### Initialize with all items shown
  searchFilter
